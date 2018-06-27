package com.iprogrammer.kafka;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class MongoSink<O> extends RichSinkFunction<Oplog> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    @Autowired
    MongoTemplate mongoTemplate;

    private MongoCollection<Document> coll;

    @Override
    public void invoke(Oplog value, Context context) throws Exception {
        LOGGER.info("oplog value: {}", value.getO());
       // mongoTemplate.insert(value.getO(), "tableA");
        coll.insertOne(new Document(value.getO()));
    }

    @Override
    public void open(Configuration config) {
        MongoClientURI uri = new MongoClientURI(
                config.getString("mongo.output.uri",
                        "mongodb://localhost:27017/local"));
        MongoClient mongoClient = null;

        try {
            mongoClient = new MongoClient(uri);
            coll = mongoClient.getDatabase(uri.getDatabase())
                    .getCollection("testCollection");
        } catch (Exception e) {

        } finally {
           /* if (mongoClient != null)
                mongoClient.close();*/
        }


    }
}
