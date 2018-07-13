package com.iprogrammer.streaming.repository;

import com.iprogrammer.streaming.model.StreamingConfig;
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

    private static final String FIELD_SEPERATOR = ".";
    @Autowired
    MongoTemplate mongoTemplate;

    public Set getCollectionNames() {
        Set collectionNames = new TreeSet(String.CASE_INSENSITIVE_ORDER);
        collectionNames.addAll(mongoTemplate.getCollectionNames());
        return collectionNames;
    }

    public Map getCollectionFields(String collectionName) {

        Document dbObject = mongoTemplate.findOne(new Query(), Document.class, collectionName);

        Map<String, String> keyValues = new LinkedHashMap();

        getKeyValuesFromDocument("", dbObject, keyValues);

        return keyValues;
    }

    void getKeyValuesFromDocument(String prefix, Document dbObject, Map<String, String> keyValues) {

        boolean isInitialIteration = true;

        if (StringUtils.isNotEmpty(prefix))
            isInitialIteration = false;

        if (dbObject != null) {

            Set<String> keyset = dbObject.keySet();

            if (keyset != null && keyset.contains("_class")) {
                keyset.remove("_class");
            }

            for (String key : keyset) {

                if (isInitialIteration)
                    prefix = "";

                if (StringUtils.isNotEmpty(prefix) && !prefix.endsWith(FIELD_SEPERATOR))
                    prefix = prefix + FIELD_SEPERATOR;

                if (StringUtils.isNotEmpty(key)) {

                    Object value = dbObject.get(key);

                    String type = getType(value);

                    if (!type.equals(Constant.OBJECT)) {
                        keyValues.put(prefix + key, type);
                    }

                    if (value instanceof Document) {

                        if (StringUtils.isNotEmpty(prefix) && !prefix.endsWith(FIELD_SEPERATOR))
                            prefix = prefix + FIELD_SEPERATOR;

                        getKeyValuesFromDocument(prefix + key + FIELD_SEPERATOR, (Document) value, keyValues);

                    } else if (value instanceof List) {

                        List arrDocuments = ((List) value);

                        for (int i = 0; i < arrDocuments.size(); i++) {

                            if (arrDocuments.get(i) instanceof Document) {

                                if (i == 0) {

                                    if (StringUtils.isNotEmpty(prefix) && !prefix.endsWith(FIELD_SEPERATOR))
                                        prefix = prefix + FIELD_SEPERATOR;

                                }

                                getKeyValuesFromDocument(prefix + key + FIELD_SEPERATOR, (Document) arrDocuments.get(i), keyValues);
                            }
                        }
                    }

                }

            }

        }

    }


    String getType(Object value) {
        if (value == null || value.equals("") || value instanceof String)
            return Constant.STRING;
        else if (value instanceof Double || value instanceof Integer || value instanceof Long)
            return Constant.INT;
        else if (value instanceof Date)
            return Constant.DATE;
        else if (value instanceof Boolean)
            return Constant.BOOLEAN;
        else if (value instanceof ObjectId)
            return Constant.OBJECT_ID;
        else return Constant.OBJECT;
    }

    public void saveStreamingConfig(StreamingConfig streamingConfig) {
        mongoTemplate.save(streamingConfig);
    }
}
