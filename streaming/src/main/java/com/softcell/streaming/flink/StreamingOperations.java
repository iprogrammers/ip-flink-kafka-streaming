package com.softcell.streaming.flink;


import com.mongodb.BasicDBObject;
import com.softcell.domains.Oplog;
import com.softcell.rest.service.StreamingConfigService;
import com.softcell.streaming.kafka.KafkaEventOplogSchema;
import com.softcell.streaming.kafka.KafkaUtil;
import com.softcell.streaming.oplog.MongoDBOplogSource;
import com.softcell.streaming.oplog.MongoSink;
import com.softcell.streaming.utils.BoundedOutOfOrdernessGenerator;
import com.softcell.utils.Constant;
import com.softcell.utils.FieldName;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Properties;

@Service
public class StreamingOperations {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private MongoSink mongoSink;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    @Value("${kafka.port}")
    private int kafkaPort;

    @Value("${kafka.group-id}")
    private String kafkaGroupId;

    @Value("${zookeeper.ip}")
    private String zookeeperIp;

    @Value("${zookeeper.port}")
    private String zookeeperPort;

    @Value("${spring.data.mongodb.database}")
    private String databaseName;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${flink.webui.port}")
    private int flinkWebUIPort;

    int getNumberOfPartitions() {
        try (KafkaUtil kafkaTopicService = new KafkaUtil(zookeeperIp + ":" + zookeeperPort, zookeeperIp, kafkaPort, 6000, 6000)) {
/*        kafkaTopicService.createOrUpdateTopic(KAFKA_TOPIC, 1, 10);*/
            return kafkaTopicService.getNumPartitionsForTopic(kafkaTopic);
        } catch (Exception e) {
            return 1;
        }
    }

    protected void createTopicIfNotExist(String topic, String zookeeper, String kafkaServer, int kafkaPort) {
        KafkaUtil kafkaTopicService = new KafkaUtil(zookeeper, kafkaServer, kafkaPort, 6000, 6000);

        logger.info("streaming topic: {} partition count: {} ", topic, kafkaTopicService.getNumPartitionsForTopic(topic));
        kafkaTopicService.createOrUpdateTopic(topic, 1, 3);
        try {
            kafkaTopicService.close();
        } catch (IOException ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);

        }
    }

    public void startConsumingOplog(ApplicationContext context) {
        new Thread() {
            @Override
            public void run() {
                try {
                    context.getBean(MongoDBOplogSource.class).run();
                } catch (Exception ex) {
                    logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
                }

            }
        }.start();

    }

    public void startStreamingOperation() {
        new Thread() {
            @Override
            public void run() {

                logger.debug("Running Flink Application");
                Configuration config = new Configuration();
                config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

                config.setInteger(RestOptions.PORT, flinkWebUIPort);

                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
                env.setParallelism(getNumberOfPartitions());
                env.enableCheckpointing(5000);
                env.getConfig().disableSysoutLogging();
                env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
                env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

                Properties properties = new Properties();
                properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
                properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "60000");

                FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010(kafkaTopic, new KafkaEventOplogSchema(), properties);
                kafkaConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

                logger.debug("metadata: {}", StreamingConfigService.STREAMING_CONFIG_META);

                DataStream<Oplog> streamSource = env
                        .addSource(kafkaConsumer);

                StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

                // register the function
                tableEnv.registerFunction("accessBasicDBObject", new AccessBasicDBObject());
                Table table1 = tableEnv.fromDataStream(streamSource);

                Table customerMISMaster = table1.filter("ns === '" + databaseName + "." + "customerMISMaster'").select("accessBasicDBObject(o,'loanApplicationId') as primaryKey, o as master ");
                Table customerMISChild1 = table1.filter("ns === '" + databaseName + "." + "customerMISChild1'").select("accessBasicDBObject(o,'loanApplicationId') as foreignKey, o  as child1");
                Table customerMISChild2 = table1.filter("ns === '" + databaseName + "." + "customerMISChild2'").select("accessBasicDBObject(o,'loanApplicationId') as foreignKey2, o as child2");

                Table result = customerMISMaster.join(customerMISChild1).where("primaryKey==foreignKey").join(customerMISChild2).where("primaryKey==foreignKey2");

                DataStream<Row> rowDataStream = tableEnv.toAppendStream(result, Row.class);

                DataStream<BasicDBObject> printStream = rowDataStream.map((Row row) -> {
                    BasicDBObject basicDBObject = new BasicDBObject();

                    for (int count = 1; count < row.getArity(); count = count + 2) {
                        if (row.getField(count) instanceof BasicDBObject) {
                            if (count > 1)
                                ((BasicDBObject) row.getField(count)).remove(FieldName.DOCUMENT_ID);
                            basicDBObject.putAll(((BasicDBObject) row.getField(count)).toMap());
                        }
                    }

                    if (basicDBObject.get("_class") != null)
                        basicDBObject.remove("_class");

                    basicDBObject.put(Constant.DERIVED_CLASS_NAME, "customerMISTesting");

                    return basicDBObject;
                });

                printStream.addSink(mongoSink);
                printStream.print();

//                DataStream<BasicDBObject> gonogoCustomerApplicationStream = streamSource.map(Oplog::getO);

             /*   DataStream<BasicDBObject> gonogoCustomerApplicationStream = streamSource.map(new MapFunction<Oplog, BasicDBObject>() {
                    @Override
                    public BasicDBObject map(Oplog oplog) throws Exception {
                        BasicDBObject basicDBObject=oplog.getO();
                        basicDBObject.put(Constant.DERIVED_CLASS_NAME,"test");
                        return basicDBObject;
                    }
                });*/


                DataStream<BasicDBObject> gonogoCustomerApplicationStream = streamSource.map((Oplog oplog) -> {
                    BasicDBObject basicDBObject = oplog.getO();
                    basicDBObject.put(Constant.DERIVED_CLASS_NAME, "test");
                    return basicDBObject;
                });

                gonogoCustomerApplicationStream.addSink(mongoSink);

                try {
                    env.execute("CYOD");
                } catch (Exception ex) {
                    logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
                }
            }
        }.start();
    }
}
