package com.softcell.rest.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.softcell.domains.CustomerMIS;
import com.softcell.domains.JavaScript;
import com.softcell.domains.StreamingConfig;
import com.softcell.domains.request.StreamingConfigRequest;
import com.softcell.domains.response.Response;
import com.softcell.persistance.StreamingOperationsRepository;
import com.softcell.persistance.helper.MongoQueryBuilder;
import com.softcell.rest.utils.Utils;
import com.softcell.utils.Constant;
import com.softcell.utils.URLEndPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.softcell.utils.Constant.STREAMING_CONFIG_NOT_FOUND;

@Service
public class StreamingConfigService {

    public static final Map<String, Map> STREAMING_CONFIG_META = new HashMap();

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingConfigService.class);

    @Autowired
    StreamingOperationsRepository streamingOperationsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    MongoQueryBuilder mongoQueryBuilder;

    public Response getCollectionNames() {

        LOGGER.debug("Method Name: [{}]", URLEndPoints.GET_COLLECTION_NAMES);

        Response.Builder builder = new Response.Builder();

        try {
            Set<String> collectionNames = streamingOperationsRepository.getCollectionNames();

            Map data = new HashMap();

            if (!CollectionUtils.isEmpty(collectionNames)) {
                String collectionName = collectionNames.iterator().next();
                Map collectionFields = streamingOperationsRepository.getCollectionFields(collectionName);

                data.put("collectionNames", collectionNames);
                data.put("defaultCollection", collectionName);
                data.put("fields", collectionFields);
            }

            if (collectionNames != null)
                return Utils.getSuccessResponseWithData(builder, data);

            return Utils.getFailedResponseStatus(builder);

        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response getFieldsFromCollection(String collectionName) {

        LOGGER.debug(Constant.LOG_REQUEST_FORMAT, URLEndPoints.GET_COLLECTION_FIELDS, collectionName);

        Response.Builder builder = new Response.Builder();

        try {

            Map collectionFields = streamingOperationsRepository.getCollectionFields(collectionName);

            if (collectionFields != null)
                return Utils.getSuccessResponseWithData(builder, collectionFields);

            return Utils.getFailedResponseStatus(builder);

        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response saveStreamingConfig(StreamingConfig streamingConfig) {

        LOGGER.debug("Method Name: [{}] Request: [{}]", URLEndPoints.CREATE_STREAMING_CONFIG, streamingConfig);

        Response.Builder builder = new Response.Builder();

        try {

            if (streamingOperationsRepository.getStreamingConfigDetailsByName(streamingConfig.getName()) == null) {

                String isValidRelationship = streamingOperationsRepository.isValidRelationshipExists(streamingConfig);

                if (!isValidRelationship.equals(Constant.SUCCESS)) {
                    return Utils.getFailedResponseStatus(builder, isValidRelationship);
                }

                streamingConfig.setStatus(Constant.STATUS_ENABLED);
                streamingConfig.setJobStatus(Constant.JOB_CREATED);

                streamingOperationsRepository.saveStreamingConfig(streamingConfig);

                return Utils.getSuccessResponseWithData(builder, streamingConfig);

            } else
                return Utils.getFailedResponseStatus(builder, Constant.DUPLICATE_STREAMING_CONFIG_NAME);

        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response updateStreamingConfig(StreamingConfig streamingConfig, boolean isUpdateStatus, String apiURL) {

        LOGGER.debug(Constant.LOG_REQUEST_FORMAT, apiURL, streamingConfig);

        Response.Builder builder = new Response.Builder();

        try {

            if (!isUpdateStatus) {

                String isValidRelationship = streamingOperationsRepository.isValidRelationshipExists(streamingConfig);

                if (!isValidRelationship.equals(Constant.SUCCESS)) {
                    return Utils.getFailedResponseStatus(builder, isValidRelationship);
                }
            }

            StreamingConfig updatedStreamingConfig = streamingOperationsRepository.updateStreamingConfig(streamingConfig, isUpdateStatus);

            if (updatedStreamingConfig != null)
                return Utils.getSuccessResponseWithData(builder, streamingConfig);
            else
                return Utils.getFailedResponseStatus(builder, STREAMING_CONFIG_NOT_FOUND + streamingConfig.getId());

        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response getStreamingConfigDetails(String id) {

        LOGGER.debug(Constant.LOG_REQUEST_FORMAT, URLEndPoints.GET_STREAMING_CONFIG_DETAILS, id);

        Response.Builder builder = new Response.Builder();

        try {

            StreamingConfig streamingConfig = streamingOperationsRepository.getStreamingConfigDetails(id);

            if (streamingConfig != null)
                return Utils.getSuccessResponseWithData(builder, streamingConfig);
            else
                return Utils.getFailedResponseStatus(builder, STREAMING_CONFIG_NOT_FOUND + id);

        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response getStreamingConfigList(StreamingConfigRequest streamingConfigRequest) {

        LOGGER.debug("Method Name: [{}] ", URLEndPoints.GET_STREAMING_CONFIG_LIST);

        Response.Builder builder = new Response.Builder();

        try {

            Query query = mongoQueryBuilder.buildStreamingConfigListQuery(streamingConfigRequest);

            Map streamingConfigList = streamingOperationsRepository.getStreamingConfigList(query);


            if (streamingConfigList != null)
                return Utils.getSuccessResponseWithData(builder, streamingConfigList);
            else
                return Utils.getFailedResponseStatus(builder, "Unable to find streaming configs.");

        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response testJavascript(JavaScript script) {

        LOGGER.debug(Constant.LOG_REQUEST_FORMAT, URLEndPoints.TEST_JAVASCRIPT, script);

        Response.Builder builder = new Response.Builder();

        try {

            Object result = Utils.runJavascripCode(script);

            if (result != null)
                return Utils.getSuccessResponseWithData(builder, result);
            else
                return Utils.getFailedResponseStatus(builder, "Something went wrong.");

        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder, ex.getMessage());
        }
    }

    public Response testRelation(CustomerMIS customerMIS) {

        LOGGER.debug(Constant.LOG_REQUEST_FORMAT, URLEndPoints.TEST_JAVASCRIPT, customerMIS);

        Response.Builder builder = new Response.Builder();

        try {

            boolean result = streamingOperationsRepository.saveRelationTemplate(customerMIS);

            return Utils.getSuccessResponseWithData(builder, result);


        } catch (Exception ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder, ex.getMessage());
        }
    }

}
