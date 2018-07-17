package com.softcell.persistance;


import com.mongodb.BasicDBObject;
import com.softcell.domains.FieldConfig;
import com.softcell.domains.StreamingConfig;
import com.softcell.utils.Constant;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.util.*;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.unwind;

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

    public void updateStreamingConfig(StreamingConfig streamingConfig) {

        StreamingConfig existingRecord = getStreamingConfigDetails(streamingConfig.getId());

        if (existingRecord != null) {
            String id = existingRecord.getId();
            long version = existingRecord.getVersion();
            existingRecord = streamingConfig;
            existingRecord.setId(id);
            existingRecord.setVersion(version);
            saveStreamingConfig(streamingConfig);
        }
    }

    public StreamingConfig getStreamingConfigDetails(String id) {
        Query query = new Query();
        query.addCriteria(Criteria.where(Constant.DOCUMENT_ID).is(id));
        return mongoTemplate.findOne(query, StreamingConfig.class, "streamingConfig");
    }

    public StreamingConfig getStreamingConfigDetailsByName(String name) {
        Query query = new Query();
        query.addCriteria(Criteria.where(Constant.NAME).is(name));
        return mongoTemplate.findOne(query, StreamingConfig.class, "streamingConfig");
    }

    public String isValidRelationshipExists(StreamingConfig streamingConfig) {

        if (streamingConfig.getFields() != null && streamingConfig.getFields().size() > 1 && streamingConfig.getFields().containsKey(streamingConfig.getName()))
            streamingConfig.getFields().remove(streamingConfig.getName());

        int numberOfCollections = streamingConfig.getFields().size();

        if (numberOfCollections <= 1)
            return Constant.SUCCESS;

        FieldConfig primaryCollection = null;

        List<FieldConfig> foreignKeys = new ArrayList<>();

        for (Map.Entry<String, List<FieldConfig>> entry : streamingConfig.getFields().entrySet()) {

            Optional<FieldConfig> primaryCollectionOptional = entry.getValue().stream().filter(FieldConfig::isPrimaryKey).findFirst();

            Optional<FieldConfig> foreignCollectionOptional = entry.getValue().stream().filter(FieldConfig::isForeignKey).findFirst();

            if (primaryCollectionOptional.isPresent())
                primaryCollection = primaryCollectionOptional.get();
            else if (foreignCollectionOptional.isPresent())
                foreignKeys.add(foreignCollectionOptional.get());
        }

        if (primaryCollection == null)
            return Constant.PRIMARY_KEY_NOT_FOUND_EXCEPTION;

        if (foreignKeys.size() != numberOfCollections - 1)
            return Constant.FOREIGN_KEY_NOT_FOUND_EXCEPTION;

        List<AggregationOperation> lookupOperationList = new LinkedList<>();

        LookupOperation lookupOperation;

        for (FieldConfig fieldConfig : foreignKeys) {

            lookupOperation = LookupOperation.newLookup().
                    from(fieldConfig.getCollectionName()).
                    localField(primaryCollection.getActualParameter()).
                    foreignField(fieldConfig.getActualParameter()).
                    as(fieldConfig.getCollectionName());

            lookupOperationList.add(lookupOperation);

            lookupOperationList.add(unwind(fieldConfig.getCollectionName()));
        }


        LimitOperation limitOperation = new LimitOperation(1);

        lookupOperationList.add(limitOperation);

        Aggregation aggregation = Aggregation.newAggregation(lookupOperationList);

        List<BasicDBObject> basicDBObjectList = mongoTemplate.aggregate(aggregation, primaryCollection.getCollectionName(), BasicDBObject.class).getMappedResults();

        if (!CollectionUtils.isEmpty(basicDBObjectList))
            return Constant.SUCCESS;
        else
            return Constant.INVALID_RELATIONSHIP_EXCEPTION;
    }

    public List<StreamingConfig> getStreamingConfigList() {
        return mongoTemplate.findAll(StreamingConfig.class);
    }
}
