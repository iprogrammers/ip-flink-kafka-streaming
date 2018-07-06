package com.iprogrammer.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//import org.springframework.data.mongodb.core.MongoTemplate;

@Component
public class MongoSink extends RichSinkFunction<BasicDBObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String mongoUri = "mongodb://localhost:27017/local";
    private static final String mongoDbName = "local";
    //    MongoCollection coll;

    @Autowired
    transient MongoTemplate mongoTemplate;

    ExecutorService executor;

    @Override
    public void invoke(BasicDBObject oplog, Context context) throws Exception {

        Observable.just(oplog)
                .subscribeOn(Schedulers.from(executor)).subscribe(t -> {
            try {
                if (mongoTemplate != null) {
                    mongoTemplate.save(oplog, "temp");
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error(e.getMessage());
            }
        });

    }

    @Override
    public void open(Configuration config) {

        executor = Executors.newFixedThreadPool(1);

        SimpleMongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoClient(), mongoDbName);

        mongoTemplate = new MongoTemplate(mongoDbFactory);

      /*  MongoClientURI uri = new MongoClientURI(
                config.getString("mongo.output.uri",
                        "mongodb://localhost:27017/local"));
        MongoClient mongoClient;
        try {
            mongoClient = new MongoClient(uri);

            coll = mongoClient.getDatabase(uri.getDatabase())
                    .getCollection("testCollection");
        } catch (Exception ex) {
            LOGGER.error("EXCEPTION: {}", ex);
        }*/
    }

    @Bean("mongoClientSecondary")
    public MongoClient mongoClient() {
        return new MongoClient(mongoClientURI());
    }

    @Bean("mongoClientURISecondary")
    public MongoClientURI mongoClientURI() {
        LOGGER.debug(" creating connection with mongodb with uri [{}] ", mongoUri);
        return new MongoClientURI(mongoUri);
    }

}
