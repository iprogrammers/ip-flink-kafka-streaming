package com.softcell.streaming.streaming.mongodb.oplog.source;

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

import com.softcell.streaming.model.MongoClientWrapper;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.softcell.streaming.model.MongoDBConstants.*;
import static com.mongodb.client.model.Filters.*;

public class MongoOplogTailMapper {
    private final String host;
    private final int port;
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    MongoTemplate mongoTemplate;
    private Integer replicaDepth;

    public MongoOplogTailMapper(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Map<String, FindPublisher<Document>> establishMongoPublishers(
            MongoCollection<Document> tsCollection) {

        Map<String, FindPublisher<Document>> publishers = new HashMap<>();

        try (MongoClient mongoS = new MongoClient(host, port)) {

            for (List<MongoClientWrapper> clients : new ShardSetFinder().findShardSets()
                    .values()) {

                if (null == getReplicaDepth())
                    setReplicaDepth(clients.size());

                bindHostToPublisher(tsCollection, publishers, clients);
            }
        }
        return publishers;
    }

    private void bindHostToPublisher(MongoCollection<Document> tsCollection,
                                     Map<String, FindPublisher<Document>> publishers, List<MongoClientWrapper> clients) {
        for (MongoClientWrapper client : clients) {
            logger.info("------------ Binding " + client.getHost() + " to oplog. ---------------");
            FindPublisher<Document> oplogPublisher = client.getClient().getDatabase("local")
                    .getCollection("oplog.rs").find().filter(getQueryFilter(tsCollection, client))
                    .sort(new Document("$natural", 1)).cursorType(CursorType.TailableAwait);
            publishers.put(client.getHost(), oplogPublisher);
        }
    }

    private Bson getFilterLastTimeStamp(MongoCollection<Document> tsCollection,
                                        MongoClientWrapper client) {
        Document lastTimeStamp = tsCollection.find(new Document("_id", client.getHost())).limit(1)
                .first();
        return getTimeQuery(lastTimeStamp == null ? null : (BsonTimestamp) lastTimeStamp
                .get(OPLOG_TIMESTAMP));
    }

    private Bson getQueryFilter(MongoCollection<Document> tsCollection, MongoClientWrapper client) {
        return and(ne(OPLOG_NAME_SPACE, "local.oplogTimestamp"),eq(OPLOG_NAME_SPACE,"gonogo_analytics_config.goNoGoCustomerApplication"),
                in(OPLOG_OPERATION, OPLOG_OPERATION_INSERT, OPLOG_OPERATION_UPDATE, OPLOG_OPERATION_DELETE), exists("fromMigrate", false),
                getFilterLastTimeStamp(tsCollection, client));
    }

    private Bson getTimeQuery(BsonTimestamp lastTimeStamp) {
        return lastTimeStamp == null ? new Document() : gt(OPLOG_TIMESTAMP, lastTimeStamp);
    }

    public Integer getReplicaDepth() {
        return replicaDepth;
    }

    public void setReplicaDepth(Integer replicaDepth) {
        this.replicaDepth = replicaDepth;
    }
}
