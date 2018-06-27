package com.iprogrammer.kafka;

/**
 * This file is part of flink-mongo-tail.
 * <p>
 * flink-mongo-tail is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * flink-mongo-tail is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with flink-mongo-tail.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @Author Jai Hirsch
 * @github https://github.com/JaiHirsch/flink-mingo-tail
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import rx.RxReactiveStreams;
import rx.schedulers.Schedulers;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.iprogrammer.kafka.MongoDBConstants.OPLOG_ID;
import static com.iprogrammer.kafka.MongoDBConstants.OPLOG_TIMESTAMP;

@Component
public class MongoDBOplogSource {

    private static final long serialVersionUID = 1140284841495470127L;
    private final String host = "localhost";
    private final int port = 27017;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass().getName());
    ConcurrentMap<Long, AtomicInteger> documentCounter = new ConcurrentHashMap<Long, AtomicInteger>();
    MongoCollection<Document> tsCollection;
    private volatile boolean isRunning = true;
    private BlockingQueue<Document> opsQueue = new ArrayBlockingQueue<Document>(128);
    private Integer replicaDepth;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public MongoDBOplogSource() {
    }

    @Autowired
    ObjectMapper objectMapper;
    public void run() throws Exception {

        try (MongoClient mongoClient = new MongoClient(host, port)) {

            tsCollection = mongoClient.getDatabase("local").getCollection(
                    "oplogTimestamp");

            MongoOplogTailMapper mongoOplogTailMapper = new MongoOplogTailMapper(host, port);

            Map<String, FindPublisher<Document>> publishers = mongoOplogTailMapper
                    .establishMongoPublishers(tsCollection);

            this.replicaDepth = mongoOplogTailMapper.getReplicaDepth();

            ExecutorService executor = Executors.newFixedThreadPool(publishers.size());

            for (Entry<String, FindPublisher<Document>> publisher : publishers.entrySet()) {
                bindPublisherToObservable(publisher, executor, tsCollection);
            }

            while (isRunning) {

                Document operation = opsQueue.poll(5, TimeUnit.SECONDS);

                if (operation == null)
                    continue;

                Oplog oplog=objectMapper.convertValue(operation,Oplog.class);
//                oplog.setTimeStamp(oplog.getTs().getValue());
              /*  OplogDTO oplogDTO=new OplogDTO();
                oplogDTO.setTs(oplog.getTs().getValue());
                oplogDTO.setO(oplog.getO());
                oplogDTO.setOp(oplog.getOp());
*/


//                sendToKafka(oplog.toString());
                sendToKafka(new ObjectMapper().writeValueAsString(oplog));

            }

            LOGGER.info("!!!!!!!!!!!!!!!!! exiting data poll isRunning = " + isRunning);
//            executor.shutdownNow();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public void sendToKafka(String payload) {
//        LOGGER.info("sending payload='{}' to topic='{}'", payload, "helloworld.t}");
        kafkaTemplate.send("helloworld.t",payload);

      /*  FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                "localhost:9092",            // broker list
                "helloworld.t",                  // target topic
                new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);*/
        /*try {
            myProducer.invoke(payload);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    int delay=10001;

    private void bindPublisherToObservable(Entry<String, FindPublisher<Document>> oplogPublisher,
                                           ExecutorService executor, MongoCollection<Document> tsCollection) {

        RxReactiveStreams.toObservable(oplogPublisher.getValue())/*.delay(delay,TimeUnit.MILLISECONDS)*/
                .subscribeOn(Schedulers.from(executor)).subscribe(t -> {
            try {
                isRunning = true;
                putOperationOnOpsQueue(oplogPublisher, tsCollection, t);

            } catch (InterruptedException e) {
                e.printStackTrace();
                LOGGER.error(e.getMessage());
            }
        });
    }

    private void putOperationOnOpsQueue(Entry<String, FindPublisher<Document>> publisher,
                                        MongoCollection<Document> tsCollection, Document t) throws InterruptedException {

        updateHostOperationTimeStamp(tsCollection, t.get(OPLOG_TIMESTAMP, BsonTimestamp.class),
                publisher.getKey());

        putOperationOnOpsQueueIfFullyReplicated(t);

    }

    private void putOperationOnOpsQueueIfFullyReplicated(Document t) throws InterruptedException {
        try {
            Long opKey = t.getLong(OPLOG_ID);
            documentCounter.putIfAbsent(opKey, new AtomicInteger(1));
            if (documentCounter.get(opKey).getAndIncrement() >= replicaDepth) {
                opsQueue.put(t);
                documentCounter.remove(opKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        }
    }


    private void updateHostOperationTimeStamp(MongoCollection<Document> tsCollection,
                                              BsonTimestamp lastTimeStamp, String host) {
        try {

            tsCollection.replaceOne(new Document("_id", host),
                    new Document("_id", host).append(OPLOG_TIMESTAMP, lastTimeStamp),
                    (new UpdateOptions()).upsert(true));
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }


   /* @Override
    public void cancel() {
        isRunning = false;

    }
*/
}
