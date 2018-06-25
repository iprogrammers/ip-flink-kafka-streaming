package com.iprogrammer.kafka;

import com.iprogrammer.kafka.utils.KafkaUtil;
import com.mongodb.BasicDBObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;

@SpringBootApplication
public class Application {

    private static final String ZOOKEEPER_IP = "127.0.0.1";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String KAFKA_TARGET_TOPIC = "employees";
    private static final String KAFKA_PRODUCER_PORT = "9092";

    @Autowired
    MongoTemplate mongoTemplate;

    public static void main(String[] args) throws Exception {

        ApplicationContext context = SpringApplication.run(Application.class);

        createTopicIfNotExist(KAFKA_TARGET_TOPIC, ZOOKEEPER_IP + ":" + ZOOKEEPER_PORT, ZOOKEEPER_IP, KAFKA_PRODUCER_PORT);

        Thread t1 = new Thread() {
            public void run() {
                try {
                    context.getBean(MongoDBOplogSource.class).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        };

        Thread t2 = new Thread() {
            public void run() {

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.enableCheckpointing(3000);
                env.getConfig().disableSysoutLogging();
                env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
                env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "localhost:9092");

                properties.setProperty("zookeeper.connect", "localhost:2181");
                properties.setProperty("group.id", "helloworld");

                FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010("helloworld.t", new KafkaEventOplogSchema(), properties);

                kafkaConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

                DataStream<Oplog> streamSource = env
                        .addSource(kafkaConsumer)
                        .setParallelism(4);

                DataStream<String> dataStream = streamSource.map(new MapFunction<Oplog, String>() {
                    @Override
                    public String map(Oplog value) throws Exception {
                        BasicDBObject document = value.getO();
                        if (document.get("SNO") != null)
                            return (String) document.get("SNO");
                        else return "";
                    }
                });

                dataStream.print();

                try {
                    env.execute("MongoDB Sharded OplogDTO Tail");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        t1.start();
        t2.start();

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    protected static void createTopicIfNotExist(String topic, String zookeeper, String kafkaServer, String kafkaPort) {
        KafkaUtil kafkaTopicService = new KafkaUtil(zookeeper, kafkaServer, Integer.valueOf(kafkaPort), 6000, 6000);

        LOGGER.info("kafka topic: {} partition count: {} " ,topic,kafkaTopicService.getNumPartitionsForTopic(topic));
        kafkaTopicService.createOrUpdateTopic(topic, 1, 3);
        try {
            kafkaTopicService.close();
        } catch (IOException io) {
            System.out.println("Error closing kafkatopicservice");
            io.printStackTrace();
        }
    }

    public static long getTimeStamp(Oplog document) {
//        Map timeStamp = (Map) document.get("ts");
//        return (long) timeStamp.get("value");
        return (long) document.getTs().getValue() >> 32;
    }

    private static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Oplog> {

        private final long maxTimeLag = 5000; // 5 seconds

        @Override
        public long extractTimestamp(Oplog element, long previousElementTimestamp) {
            return getTimeStamp(element);
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }

    /**
     * A custom {@link AssignerWithPeriodicWatermarks}, that simply assumes that the input stream
     * records are strictly ascending.
     * <p>
     * <p>Flink also ships some built-in convenience assigners, such as the
     * {@link BoundedOutOfOrdernessTimestampExtractor} and {@link AscendingTimestampExtractor}
     */
    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<Oplog> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(Oplog event, long previousElementTimestamp) {
            // the inputs are assumed to be of format (message,timestamp)


            this.currentTimestamp = getTimeStamp(event);
            return currentTimestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
