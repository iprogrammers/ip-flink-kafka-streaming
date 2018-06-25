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

import com.mongodb.*;
import com.mongodb.reactivestreams.client.MongoClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.*;

public class ShardSetFinder {

    @Autowired
    MongoTemplate mongoTemplate;
    private List<MongoClientWrapper> mongoClients;

    public Map<String, List<MongoClientWrapper>> findShardSets(MongoClient mongoS) {
        // TODO figure out how to do this with the new driver syntax. Does not
        // appear to support sisterDB
      /*DBCursor find = mongoS.getDB("local").getSisterDB("config").getCollection("shards").find();*/

      /*  DBCollection collection = mongoTemplate.getDb().getSisterDB("config").getCollection("shards");
        DBCursor find = collection.find();*/
        Map<String, List<MongoClientWrapper>> shardSets = new HashMap<>();
       /* while (find.hasNext()) {
            DBObject next = find.next();
            String key = (String) next.get("_id");
            shardSets.put(key, getMongoClient(buildServerAddressList(next)));
        }*/
        shardSets.put("amol", getMongoClient(buildServerAddressList()));
//        find.close();
        return shardSets;
    }

    private List<MongoClientWrapper> getMongoClient(List<ConnectionString> shardSet) {
        mongoClients = new ArrayList<>();
        try {
            for (ConnectionString address : shardSet) {
                com.mongodb.reactivestreams.client.MongoClient client=MongoClients.create(address);
                mongoClients.add(new MongoClientWrapper(address.getConnectionString(), client));
                Thread.sleep(100); // allow the client to establish prior to being
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return mongoClients;
    }

    private List<ConnectionString> buildServerAddressList() {
        List<ConnectionString> hosts = new ArrayList<>();
//        for (String host : Arrays.asList(((String) next.get("host")).split("/")[1].split(","))) {
            hosts.add(new ConnectionString("mongodb://localhost:27017" ));
//        }
        return hosts;
    }

}
