package com.iprogrammer.kafka;

import com.mongodb.*;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

@Service
public class Test {

    private static final String HELLOWORLD_TOPIC = "helloworld.t";

    public static BSONTimestamp lastTimeStamp;

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    private Sender sender;

    private static Logger logger = LoggerFactory.getLogger(Test.class);

    void test() {

     /*   DBObject queryCondition = new BasicDBObject();

        BasicDBList values = new BasicDBList();
        values.add(new BasicDBObject("op", "i"));
        values.add(new BasicDBObject("op", "u"));
        values.add(new BasicDBObject("op", "d"));
        queryCondition.put("$or", values);

        if (lastTimeStamp != null) {

            queryCondition.put("ts",
                    new BasicDBObject("$gt", lastTimeStamp));

        }else {
            OplogTimestamp oplogTimestamp=mongoTemplate.findOne(null,OplogTimestamp.class);
            if (oplogTimestamp!=null){
                lastTimeStamp=oplogTimestamp.getLastTimeStamp();
            }
        }

        final DBCursor cur = mongoTemplate.getCollection("oplog.rs")
                .find(queryCondition)
                .sort(new BasicDBObject("$natural", 1))
                .addOption(Bytes.QUERYOPTION_TAILABLE)
                .addOption(Bytes.QUERYOPTION_AWAITDATA);

        new Thread() {
            public void run() {
                //cur.hasNext will wait for data
                while (cur.hasNext()) {
                    BasicDBObject obj = (BasicDBObject) cur.next();
                    lastTimeStamp= (BSONTimestamp) obj.get("ts");
                    logger.debug("timestamp {}",lastTimeStamp.getTime());
                    sender.send(HELLOWORLD_TOPIC, obj.toString());
                    System.out.println(obj);
                }
            }

            ;
        }.start();*/
    }


}
