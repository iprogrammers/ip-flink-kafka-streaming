package com.softcell.streaming.oplog;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.softcell.utils.Constant;
import com.softcell.utils.FieldName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class MongoSink extends RichSinkFunction<BasicDBObject> {

    private static final transient Logger LOGGER = LoggerFactory.getLogger(MongoSink.class);

    @Autowired
    transient MongoTemplate mongoTemplate;

    transient ExecutorService executor;

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.data.mongodb.database}")
    private String mongoDbName;

    @Override
    public void invoke(BasicDBObject basicDBObject, Context context) throws Exception {

        Observable.just(basicDBObject)
                .subscribeOn(Schedulers.from(executor)).subscribe(t -> {
            try {
                if (mongoTemplate != null) {

                    //convert string object id to mongo object id type
                    if (basicDBObject.get(FieldName.DOCUMENT_ID) != null && basicDBObject.get(FieldName.DOCUMENT_ID) instanceof String)
                        basicDBObject.put(FieldName.DOCUMENT_ID, new ObjectId(basicDBObject.getString(FieldName.DOCUMENT_ID)));

                    basicDBObject.put(Constant.IS_DERIVED_COLLECTION,true);
                    String collectionName=basicDBObject.getString(Constant.DERIVED_CLASS_NAME);
                    basicDBObject.remove(Constant.DERIVED_CLASS_NAME);
                    mongoTemplate.save(basicDBObject, collectionName);
                }
            } catch (Exception ex) {
                LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            }
        });

    }

    @Override
    public void open(Configuration config) {

        executor = Executors.newFixedThreadPool(1);

        SimpleMongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoClient(), mongoDbName);

        mongoTemplate = new MongoTemplate(mongoDbFactory);

    }

    @Bean("mongoClientSecondary")
    public MongoClient mongoClient() {
        return new MongoClient(mongoClientURI());
    }

    @Bean("mongoClientURISecondary")
    public MongoClientURI mongoClientURI() {
        LOGGER.debug("creating connection with mongodb with uri [{}] ", mongoUri);
        return new MongoClientURI(mongoUri);
    }

}
