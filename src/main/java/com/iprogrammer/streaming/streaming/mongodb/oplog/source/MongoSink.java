package com.iprogrammer.streaming.streaming.mongodb.oplog.source;

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


@Component
public class MongoSink extends RichSinkFunction<BasicDBObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSink.class);
    private static final String MONGO_URI = "mongodb://localhost:27017/local";
    private static final String MONGO_DB_NAME = "local";

    @Autowired
    transient MongoTemplate mongoTemplate;

   transient ExecutorService executor;

    @Override
    public void invoke(BasicDBObject oplog, Context context) throws Exception {

        Observable.just(oplog)
                .subscribeOn(Schedulers.from(executor)).subscribe(t -> {
            try {
                if (mongoTemplate != null) {
                    mongoTemplate.save(oplog, "temp");
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        });

    }

    @Override
    public void open(Configuration config) {

        executor = Executors.newFixedThreadPool(1);

        SimpleMongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoClient(), MONGO_DB_NAME);

        mongoTemplate = new MongoTemplate(mongoDbFactory);

    }

    @Bean("mongoClientSecondary")
    public MongoClient mongoClient() {
        return new MongoClient(mongoClientURI());
    }

    @Bean("mongoClientURISecondary")
    public MongoClientURI mongoClientURI() {
        LOGGER.debug(" creating connection with mongodb with uri [{}] ", MONGO_URI);
        return new MongoClientURI(MONGO_URI);
    }

}
