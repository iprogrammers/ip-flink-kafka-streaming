package com.softcell.streaming.oplog;

import com.mongodb.CursorType;
import com.mongodb.client.MongoCollection;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.softcell.streaming.utils.MongoClientWrapper;
import com.softcell.utils.Constant;
import com.softcell.utils.FieldName;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;
import static com.softcell.domains.MongoDBConstants.*;
import static com.softcell.utils.Constant.IS_DERIVED_COLLECTION;

@Component
public class MongoOplogTailMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoOplogTailMapper.class);

    @Autowired
    ShardSetFinder shardSetFinder;

    private Integer replicaDepth;

    @Value("${spring.data.mongodb.database}")
    private String mongoDbName;

    public Map<String, FindPublisher<Document>> establishMongoPublishers(
            MongoCollection<Document> tsCollection) {

        Map<String, FindPublisher<Document>> publishers = new HashMap<>();

        for (List<MongoClientWrapper> clients : shardSetFinder.findShardSets()
                .values()) {

            if (null == getReplicaDepth())
                setReplicaDepth(clients.size());

            bindHostToPublisher(tsCollection, publishers, clients);
        }

        return publishers;
    }

    private void bindHostToPublisher(MongoCollection<Document> tsCollection,
                                     Map<String, FindPublisher<Document>> publishers, List<MongoClientWrapper> clients) {
        for (MongoClientWrapper client : clients) {

            LOGGER.info("Binding {} to oplog.", client.getHost());

            FindPublisher<Document> oplogPublisher = client.getClient().getDatabase(Constant.OPLOG_DB)
                    .getCollection(Constant.OPLOG_COLLECTION_NAME).find().filter(getQueryFilter(tsCollection, client))
                    .sort(new Document("$natural", 1)).cursorType(CursorType.TailableAwait);

            publishers.put(client.getHost(), oplogPublisher);
        }
    }

    private Bson getFilterLastTimeStamp(MongoCollection<Document> tsCollection,
                                        MongoClientWrapper client) {
        Document lastTimeStamp = tsCollection.find(new Document(FieldName.DOCUMENT_ID, client.getHost())).limit(1)
                .first();
        return getTimeQuery(lastTimeStamp == null ? null : (BsonTimestamp) lastTimeStamp
                .get(OPLOG_TIMESTAMP));
    }

    private Bson getQueryFilter(MongoCollection<Document> tsCollection, MongoClientWrapper client) {
        return and(ne(OPLOG_OPERATION, OPLOG_OPERATION_NO_OP),
                in(OPLOG_OPERATION, OPLOG_OPERATION_INSERT, OPLOG_OPERATION_UPDATE, OPLOG_OPERATION_DELETE),
                ne(OPLOG_NAME_SPACE, mongoDbName + "." + Constant.OPLOG_TIMESTAMP_COLLECTION),
                or(eq("o." + IS_DERIVED_COLLECTION, false), exists("o." + IS_DERIVED_COLLECTION, false)),
                exists("fromMigrate", false),
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
