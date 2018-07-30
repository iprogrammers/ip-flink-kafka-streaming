package com.softcell.streaming.oplog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.softcell.domains.Oplog;
import com.softcell.domains.OplogDocument;
import com.softcell.streaming.utils.DocumentIdSerializer;
import com.softcell.utils.Constant;
import com.softcell.utils.FieldName;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.schedulers.Schedulers;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.softcell.domains.MongoDBConstants.OPLOG_ID;
import static com.softcell.domains.MongoDBConstants.OPLOG_TIMESTAMP;


@Component
public class MongoDBOplogSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBOplogSource.class);

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    MongoOplogTailMapper mongoOplogTailMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    private ConcurrentMap<Long, AtomicInteger> documentCounter = new ConcurrentHashMap();

    private volatile boolean isRunning = true;

    private BlockingQueue<OplogDocument> opsQueue = new ArrayBlockingQueue(50);

    private Integer replicaDepth;

    public MongoDBOplogSource() {
        //Flink need no arg constructor
    }

    public void run() {

        try {

            MongoCollection<Document> tsCollection = mongoTemplate.getDb().getCollection(Constant.OPLOG_TIMESTAMP_COLLECTION);

            Map<String, FindPublisher<Document>> publishers = mongoOplogTailMapper
                    .establishMongoPublishers(tsCollection);

            this.replicaDepth = mongoOplogTailMapper.getReplicaDepth();

            ExecutorService executor = Executors.newFixedThreadPool(publishers.size());

            for (Entry<String, FindPublisher<Document>> publisher : publishers.entrySet()) {
                bindPublisherToObservable(publisher, executor, tsCollection);
            }

            SimpleModule module = new SimpleModule();
            module.addSerializer(ObjectId.class, new DocumentIdSerializer());
            objectMapper.registerModule(module);

            while (isRunning) {

                OplogDocument operation = opsQueue.poll(5, TimeUnit.SECONDS);

                if (operation == null)
                    continue;

                Observable.just(operation.getDocument())
                        .subscribeOn(Schedulers.from(executor)).subscribe(document -> {
                    try {

                        Oplog oplog = objectMapper.convertValue(document, Oplog.class);

                        sendToKafka(objectMapper.writeValueAsString(document));

                        updateHostOperationTimeStamp(tsCollection, document.get(OPLOG_TIMESTAMP, BsonTimestamp.class),
                                operation.getHost());

                    } catch (JsonProcessingException ex) {
                        LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
                    }
                });


            }

            LOGGER.info("exiting data poll isRunning {} ", isRunning);

        } catch (InterruptedException ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
        }
    }

    public void sendToKafka(String payload) {
        kafkaTemplate.send(kafkaTopic, payload);
    }

    private void bindPublisherToObservable(Entry<String, FindPublisher<Document>> oplogPublisher,
                                           ExecutorService executor, MongoCollection<Document> tsCollection) {

        RxReactiveStreams.toObservable(oplogPublisher.getValue())
                .subscribeOn(Schedulers.from(executor)).subscribe(document -> {
            try {
                isRunning = true;
                putOperationOnOpsQueue(oplogPublisher, tsCollection, document);

            } catch (InterruptedException ex) {
                LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
                // Restore interrupted state...
                Thread.currentThread().interrupt();
            }
        });
    }

    private void putOperationOnOpsQueue(Entry<String, FindPublisher<Document>> publisher,
                                        MongoCollection<Document> tsCollection, Document document) throws InterruptedException {

       /* updateHostOperationTimeStamp(tsCollection, document.get(OPLOG_TIMESTAMP, BsonTimestamp.class),
                publisher.getKey());*/

        putOperationOnOpsQueueIfFullyReplicated(document,publisher.getKey());

    }

    private void putOperationOnOpsQueueIfFullyReplicated(Document document, String host) throws InterruptedException {
        try {
            Long opKey = document.getLong(OPLOG_ID);
            documentCounter.putIfAbsent(opKey, new AtomicInteger(1));
            if (documentCounter.get(opKey).getAndIncrement() >= replicaDepth) {
                opsQueue.put(new OplogDocument(host,document));
                documentCounter.remove(opKey);
            }
        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
        }
    }


    private void updateHostOperationTimeStamp(MongoCollection<Document> tsCollection,
                                              BsonTimestamp lastTimeStamp, String host) {
        try {

            tsCollection.replaceOne(new Document(FieldName.DOCUMENT_ID, host),
                    new Document(FieldName.DOCUMENT_ID, host).append(OPLOG_TIMESTAMP, lastTimeStamp)
                            .append(Constant.IS_DERIVED_COLLECTION, true),
                    (new UpdateOptions()).upsert(true));
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}
