package com.iprogrammer.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

@Service
public class MongoSink<O> extends RichSinkFunction<BasicDBObject> {

    private static final long serialVersionUID = 1905122041950251222L;

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    @Autowired
    MongoTemplate mongoTemplate;

    private MongoCollection<Document> coll;

    @Override
    public void invoke(BasicDBObject oplog, Context context) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Document document = Document.parse(objectMapper.writeValueAsString(oplog));
        coll.insertOne(document);
    }

    @Override
    public void open(Configuration config) {
        MongoClientURI uri = new MongoClientURI(
                config.getString("mongo.output.uri",
                        "mongodb://localhost:27017/local"));

        try (MongoClient mongoClient = new MongoClient(uri)) {
            coll = mongoClient.getDatabase(uri.getDatabase())
                    .getCollection("testCollection");
        } catch (Exception ex) {
            LOGGER.error("EXCEPTION: {}", ex);
        }


    }
}
