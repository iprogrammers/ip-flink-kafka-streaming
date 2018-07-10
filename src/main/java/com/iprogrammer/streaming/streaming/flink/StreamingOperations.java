package com.iprogrammer.streaming.streaming.flink;

import com.iprogrammer.streaming.model.Oplog;
import com.iprogrammer.streaming.streaming.kafka.KafkaEventOplogSchema;
import com.iprogrammer.streaming.streaming.kafka.KafkaUtil;
import com.iprogrammer.streaming.streaming.mongodb.oplog.source.MongoDBOplogSource;
import com.iprogrammer.streaming.streaming.mongodb.oplog.source.MongoSink;
import com.iprogrammer.streaming.utils.BoundedOutOfOrdernessGenerator;
import com.iprogrammer.streaming.utils.Constant;
import com.mongodb.BasicDBObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Properties;

@Service
public class StreamingOperations {

    public static final String KAFKA_TOPIC = "helloworld.t";
    public static final String ZOOKEEPER_PORT = "2181";
    public static final int KAFKA_PRODUCER_PORT = 9092;
    public static final String ZOOKEEPER_IP = "localhost";
    static int count = 0;
    private final Logger logger = LoggerFactory.getLogger(StreamingOperations.class);
    @Autowired
    MongoSink mongoSink;

    @Autowired
    MongoTemplate mongoTemplate;

    static int getNumberOfPartitions() {
        try (KafkaUtil kafkaTopicService = new KafkaUtil(ZOOKEEPER_IP + ":" + ZOOKEEPER_PORT, ZOOKEEPER_IP, KAFKA_PRODUCER_PORT, 6000, 6000)) {
//        kafkaTopicService.createOrUpdateTopic(KAFKA_TOPIC, 1, 10);
            return kafkaTopicService.getNumPartitionsForTopic(KAFKA_TOPIC);
        } catch (Exception e) {
            return 0;
        }
    }

    protected void createTopicIfNotExist(String topic, String zookeeper, String kafkaServer, int kafkaPort) {
        KafkaUtil kafkaTopicService = new KafkaUtil(zookeeper, kafkaServer, kafkaPort, 6000, 6000);

        logger.info("streaming topic: {} partition count: {} ", topic, kafkaTopicService.getNumPartitionsForTopic(topic));
        kafkaTopicService.createOrUpdateTopic(topic, 1, 3);
        try {
            kafkaTopicService.close();
        } catch (IOException ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(),ex);

        }
    }

    public void startConsumingOplog(ApplicationContext context) {
        new Thread() {
            @Override
            public void run() {
                try {
                    context.getBean(MongoDBOplogSource.class).run();
                } catch (Exception ex) {
                    logger.error(HttpStatus.FAILED_DEPENDENCY.name(),ex);
                }

            }
        }.start();

    }

    public void startStreamingOperation() {
        new Thread() {
            @Override
            public void run() {

                Configuration config = new Configuration();
                config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

                env.enableCheckpointing(5000);
                env.getConfig().disableSysoutLogging();
                env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
                env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "localhost:9092");
                properties.setProperty("zookeeper.connect", "localhost:2181");
                properties.setProperty("group.id", "helloworld");
                properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "60000");

                FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010(KAFKA_TOPIC, new KafkaEventOplogSchema(), properties);

                kafkaConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

                DataStream<Oplog> streamSource = env
                        .addSource(kafkaConsumer)
                        .setParallelism(10).map(new MapFunction<Oplog, Oplog>() {
                            @Override
                            public Oplog map(Oplog oplog) throws Exception {

                                /*if (oplog.getNs().equals("gonogo_analytics_config.goNoGoCustomerApplication")) {
                                    oplog.setPrimaryKey(oplog.getO().getString("_id"));
                                } else*/

                                if (oplog.getNs().equals("analyticDB.customerMISMaster")) {
                                    oplog.setPrimaryKey(oplog.getO().getString(Constant.LOAN_APPLICATION_ID));
                                } else if (oplog.getNs().equals("analyticDB.customerMISChild1")) {
                                    oplog.setForeignKey(oplog.getO().getString(Constant.LOAN_APPLICATION_ID));
                                } else if (oplog.getNs().equals("analyticDB.customerMISChild2")) {
                                    oplog.setForeignKey(oplog.getO().getString(Constant.LOAN_APPLICATION_ID));
                                }
                                return oplog;
                            }
                        });

               /* StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

                Table table1 = tableEnv.fromDataStream(streamSource);*/
/*
                Table customerMISMaster = table1.filter("ns === 'analyticDB.customerMISMaster'").select("o as master, primaryKey");
                Table customerMISChild1 = table1.filter("ns === 'analyticDB.customerMISChild1'").select("o  as child1, foreignKey");
                Table customerMISChild2 = table1.filter("ns === 'analyticDB.customerMISChild2'").select("o  as child2, foreignKey as foreignKey2");

                Table result = customerMISMaster.join(customerMISChild1).where("primaryKey==foreignKey").join(customerMISChild2).where("primaryKey==foreignKey2");

                DataStream<Row> rowDataStream = tableEnv.toDataStream(result, Row.class);

                DataStream<BasicDBObject> printStream = rowDataStream.map(new MapFunction<Row, BasicDBObject>() {
                    @Override
                    public BasicDBObject map(Row row) throws Exception {

                        BasicDBObject basicDBObject = new BasicDBObject();
                        for (int count = 0; count < row.getArity(); count = count + 2) {
                            basicDBObject.putAll(((BasicDBObject) row.getField(count)).toMap());
                        }

                        basicDBObject.remove("_class");
                        return basicDBObject;
                    }
                });

                printStream.addSink(new MongoSink<BasicDBObject>());*/


//                DataStream<Oplog> rowDataStreamGonogo = tableEnv.toDataStream(table1, Oplog.class);

                DataStream<BasicDBObject> gonogoCustomerApplicationStream = streamSource.map(new MapFunction<Oplog, BasicDBObject>() {
                    @Override
                    public BasicDBObject map(Oplog oplog) throws Exception {
                      return oplog.getO();

                    }
                });

               /* DataStream<String> gonogoCustomerApplicationStream = streamSource.map(new MapFunction<Oplog, String>() {
                    @Override
                    public String map(Oplog oplog) throws Exception {
                        count=count+1;
                        BasicDBObject basicDBObject = oplog.getO();
                        if (basicDBObject.getString("_id") != null)
                            return count+"->"+basicDBObject.getString("_id");
                        else return count+"---#######------";
                    }
                });*/

//                gonogoCustomerApplicationStream.print();
                gonogoCustomerApplicationStream.addSink(mongoSink);

                try {
                    env.execute("Flink MongoDB Streaming");
                } catch (Exception ex) {
                    logger.error(HttpStatus.FAILED_DEPENDENCY.name(),ex);
                }
            }
        }.start();
    }
}
