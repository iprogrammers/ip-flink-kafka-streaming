package com.iprogrammer.kafka;

import com.iprogrammer.kafka.utils.KafkaUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.Properties;

@SpringBootApplication
public class Application {

    private static final String ZOOKEEPER_IP = "127.0.0.1";
    private static final String ZOOKEEPER_PORT = "2181";
    public static final String KAFKA_TOPIC = "helloworld.t";
    private static final String KAFKA_PRODUCER_PORT = "9092";
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    static int counter = 1;


    public static void main(String[] args) throws Exception {

        ApplicationContext context = SpringApplication.run(Application.class);

//        createTopicIfNotExist(KAFKA_TARGET_TOPIC, ZOOKEEPER_IP + ":" + ZOOKEEPER_PORT, ZOOKEEPER_IP, KAFKA_PRODUCER_PORT);

        new Thread() {
            public void run() {
                try {
                    context.getBean(MongoDBOplogSource.class).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }.start();

        new Thread() {
            public void run() {

                Configuration config = new Configuration();
                config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
             /*   StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();*/
                env.enableCheckpointing(3000);
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
                        .setParallelism(4);


                DataStream<Oplog> dataStream2 = streamSource.map(new MapFunction<Oplog, Oplog>() {
                    @Override
                    public Oplog map(Oplog oplog) throws Exception {

                        BasicDBObject document = oplog.getO();
                        document.put("SNO", document.getString("SNO") + "AAA");
                        oplog.setO(document);
                        return oplog;
                    }
                });

                DataStream<Oplog> dataStream3 = dataStream2.map(new MapFunction<Oplog, Oplog>() {
                    @Override
                    public Oplog map(Oplog oplog) throws Exception {

                        BasicDBObject document = oplog.getO();
                        document.put("SNO", document.getString("SNO") + "BBB");
                        oplog.setO(document);
                        return oplog;
                    }
                });

//                dataStream3.addSink(new MongoSink<Oplog>());

                DataStream<String> dataStream4 = dataStream3.map(new MapFunction<Oplog, String>() {
                    @Override
                    public String map(Oplog oplog) throws Exception {


                    /*    if (counter % 5 == 0 && oplog.getPartitionId() == 1) {
                            int delay = (1 + new Random().nextInt(4)) * 100;
                            Thread.sleep(delay);
                        }
*/
//                        counter++;

                        BasicDBObject document = oplog.getO();
                        document.put("SNO", document.getString("SNO") + "CCC");
                        oplog.setO(document);
                        return oplog.getO().getString("SNO");
                    }
                });

                dataStream4.print();
//                dataStream.print();

                try {
                    env.execute("Flink MongoDB Streaming");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }


    protected static void createTopicIfNotExist(String topic, String zookeeper, String kafkaServer, String kafkaPort) {
        KafkaUtil kafkaTopicService = new KafkaUtil(zookeeper, kafkaServer, Integer.valueOf(kafkaPort), 6000, 6000);

        LOGGER.info("kafka topic: {} partition count: {} ", topic, kafkaTopicService.getNumPartitionsForTopic(topic));
        kafkaTopicService.createOrUpdateTopic(topic, 1, 3);
        try {
            kafkaTopicService.close();
        } catch (IOException io) {
            System.out.println("Error closing kafkatopicservice");
            io.printStackTrace();
        }
    }

    public static long getTimeStamp(Oplog document) {
        return document.getTs().getValue() >> 32;
    }



}
