package com.iprogrammer.streaming.streaming.mongodb.oplog.source;

import com.iprogrammer.streaming.model.MongoClientWrapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardSetFinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardSetFinder.class);

    @Autowired
    MongoTemplate mongoTemplate;

    public Map<String, List<MongoClientWrapper>> findShardSets() {

        Map<String, List<MongoClientWrapper>> shardSets = new HashMap<>();
        shardSets.put("shard-set", getMongoClient(buildServerAddressList()));
        return shardSets;
    }

    private List<MongoClientWrapper> getMongoClient(List<ConnectionString> shardSet) {

        List<MongoClientWrapper> mongoClients = new ArrayList<>();

        try {
            for (ConnectionString address : shardSet) {
                com.mongodb.reactivestreams.client.MongoClient client = MongoClients.create(address);
                mongoClients.add(new MongoClientWrapper(address.getConnectionString(), client));
                Thread.sleep(100); // allow the client to establish prior to being
            }
        } catch (InterruptedException ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(),ex);
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
        return mongoClients;
    }

    private List<ConnectionString> buildServerAddressList() {
        List<ConnectionString> hosts = new ArrayList<>();
        hosts.add(new ConnectionString("mongodb://localhost:27017"));
        return hosts;
    }

}
