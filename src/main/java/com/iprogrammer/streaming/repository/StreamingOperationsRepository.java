package com.iprogrammer.streaming.repository;

import com.iprogrammer.streaming.utils.Constant;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class StreamingOperationsRepository {

    @Autowired
    MongoTemplate mongoTemplate;

    public Set getCollectionNames() {
        Set collectionNames = new TreeSet(String.CASE_INSENSITIVE_ORDER);
        collectionNames.addAll(mongoTemplate.getCollectionNames());
        return collectionNames;
    }

    public List getCollectionFields(String collectionName) {

        Document dbObject = mongoTemplate.findOne(new Query(), Document.class, collectionName);

        List<Map> dataDictionary = new ArrayList();

        if (dbObject != null) {

            Set<String> keyset = dbObject.keySet();

            HashMap keyValues;

            if (keyset != null && keyset.contains("_class")) {
                keyset.remove("_class");
            }

            for (String key : keyset) {

                if (StringUtils.isNotEmpty(key)) {

                    keyValues = new HashMap();

                    Object value = dbObject.get(key);

                    String displayParameter = StringUtils.join(
                            StringUtils.splitByCharacterTypeCamelCase(StringUtils.capitalize(key)),
                            ' '
                    );

                    keyValues.put("actualParameter", key);
                    keyValues.put("displayParameter", displayParameter);


                    if (value == null || value.equals("") || value instanceof String)
                        keyValues.put(Constant.TYPE, Constant.STRING);
                    else if (value instanceof Double || value instanceof Integer || value instanceof Long)
                        keyValues.put(Constant.TYPE, Constant.INT);
                    else if (value instanceof Date)
                        keyValues.put(Constant.TYPE, Constant.DATE);
                    else if (value instanceof Boolean)
                        keyValues.put(Constant.TYPE, Constant.BOOLEAN);
                    else if (value instanceof ObjectId)
                        keyValues.put(Constant.TYPE, Constant.OBJECT_ID);
                    dataDictionary.add(keyValues);
                }
            }

        }

        return dataDictionary;
    }
}
