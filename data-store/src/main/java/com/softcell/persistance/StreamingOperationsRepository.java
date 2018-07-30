package com.softcell.persistance;


import com.mongodb.BasicDBObject;
import com.softcell.domains.CustomerMIS;
import com.softcell.domains.FieldConfig;
import com.softcell.domains.StreamingConfig;
import com.softcell.persistance.helper.RepositoryHelper;
import com.softcell.utils.Constant;
import com.softcell.utils.FieldName;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingOperationsRepository.class);

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

        RepositoryHelper.getKeyValuesFromDocument("", dbObject, keyValues);

        return keyValues;
    }


    public void saveStreamingConfig(StreamingConfig streamingConfig) {
        mongoTemplate.save(streamingConfig);
    }

    public StreamingConfig updateStreamingConfig(StreamingConfig streamingConfig, boolean isUpdateStatus) {

        StreamingConfig existingRecord = getStreamingConfigDetails(streamingConfig.getId());

        if (existingRecord != null) {

            if (isUpdateStatus) {
                existingRecord.setStatus(streamingConfig.getStatus());
            } else {
                String id = existingRecord.getId();
                long version = existingRecord.getVersion();
                existingRecord = streamingConfig;
                existingRecord.setId(id);
                existingRecord.setVersion(version);
            }

            existingRecord.setPersisted(true);

            saveStreamingConfig(existingRecord);
            return existingRecord;
        } else
            return null;
    }


    public StreamingConfig getStreamingConfigDetails(String id) {
        Query query = new Query();
        query.addCriteria(Criteria.where(FieldName.DOCUMENT_ID).is(id));
        return mongoTemplate.findOne(query, StreamingConfig.class, "streamingConfig");
    }

    public StreamingConfig getStreamingConfigDetailsByName(String name) {
        Query query = new Query();
        query.addCriteria(Criteria.where(Constant.NAME).is(name));
        return mongoTemplate.findOne(query, StreamingConfig.class, "streamingConfig");
    }

    public String isValidRelationshipExists(StreamingConfig streamingConfig) {

        int numberOfCollections = RepositoryHelper.getNumberOfCollections(streamingConfig);

        if (numberOfCollections <= 1)
            return Constant.SUCCESS;

        List<FieldConfig> foreignKeys = new ArrayList<>();

        FieldConfig primaryCollectionField = RepositoryHelper.getPrimaryAndForeignKeysFromConfig(streamingConfig, foreignKeys);

        if (primaryCollectionField == null)
            return Constant.PRIMARY_KEY_NOT_FOUND_EXCEPTION;

        if (foreignKeys.size() != numberOfCollections - 1)
            return Constant.FOREIGN_KEY_NOT_FOUND_EXCEPTION;

        List<AggregationOperation> lookupOperationList = new LinkedList<>();

        LookupOperation lookupOperation;

        for (FieldConfig fieldConfig : foreignKeys) {

            lookupOperation = LookupOperation.newLookup().
                    from(fieldConfig.getCollectionName()).
                    localField(primaryCollectionField.getActualParameter()).
                    foreignField(fieldConfig.getActualParameter()).
                    as(fieldConfig.getCollectionName());

            lookupOperationList.add(lookupOperation);

            lookupOperationList.add(unwind(fieldConfig.getCollectionName()));
        }


        LimitOperation limitOperation = new LimitOperation(1);

        lookupOperationList.add(limitOperation);

        Aggregation aggregation = Aggregation.newAggregation(lookupOperationList);

        List<BasicDBObject> basicDBObjectList = mongoTemplate.aggregate(aggregation, primaryCollectionField.getCollectionName(), BasicDBObject.class).getMappedResults();

        if (!CollectionUtils.isEmpty(basicDBObjectList))
            return Constant.SUCCESS;
        else
            return Constant.INVALID_RELATIONSHIP_EXCEPTION;
    }

    public List<StreamingConfig> getStreamingConfigList() {
        return mongoTemplate.findAll(StreamingConfig.class);
    }

    public Map getStreamingConfigList(Query query) {

        int limit = query.getLimit();
        long totalRecords = 0;
        Map hashMap = new HashMap();
        query.limit(0);

        try {
            totalRecords = mongoTemplate.count(query, StreamingConfig.class);
        } catch (Exception ex) {
            LOGGER.error("Please contact service provide" + ex.getMessage());
        }

        query.limit(limit);

        try {

            List<StreamingConfig> streamingConfigList = mongoTemplate.find(query, StreamingConfig.class);
            hashMap.put("total", totalRecords);
            hashMap.put("data", streamingConfigList);
            return hashMap;

        } catch (Exception ex) {
            LOGGER.error("Exception : Please contact Service provider", ex);
        }

        return hashMap;

    }

    public boolean saveRelationTemplate(CustomerMIS customerMIS) {

        if (customerMIS.getCustomerMISMaster() != null)
            mongoTemplate.save(customerMIS.getCustomerMISMaster());
        if (customerMIS.getCustomerMISChild1() != null)
            mongoTemplate.save(customerMIS.getCustomerMISChild1());
        if (customerMIS.getCustomerMISChild2() != null)
            mongoTemplate.save(customerMIS.getCustomerMISChild2());
        return true;
    }

}
