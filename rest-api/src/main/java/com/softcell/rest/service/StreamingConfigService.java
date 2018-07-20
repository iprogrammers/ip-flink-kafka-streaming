package com.softcell.rest.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.softcell.domains.JavaScript;
import com.softcell.domains.StreamingConfig;
import com.softcell.domains.response.Response;
import com.softcell.persistance.StreamingOperationsRepository;
import com.softcell.rest.utils.Utils;
import com.softcell.utils.Constant;
import com.softcell.utils.URLEndPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class StreamingConfigService {

    public static final Map<String, Map> streamingConfigMeta = new HashMap();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    StreamingOperationsRepository streamingOperationsRepository;

    @Autowired
    ObjectMapper objectMapper;

    public Response getCollectionNames() {

        logger.debug("Method Name: [{}]", URLEndPoints.GET_COLLECTION_NAMES);

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
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response getFieldsFromCollection(String collectionName) {

        logger.debug("Method Name: [{}] Request:  [{}] ", URLEndPoints.GET_COLLECTION_FIELDS, collectionName);

        Response.Builder builder = new Response.Builder();

        try {

            Map collectionFields = streamingOperationsRepository.getCollectionFields(collectionName);

            if (collectionFields != null)
                return Utils.getSuccessResponseWithData(builder, collectionFields);

            return Utils.getFailedResponseStatus(builder);

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response createStreamingConfig(StreamingConfig streamingConfig) {

        logger.debug("Method Name: [{}] Request:  [{}] ", URLEndPoints.GET_COLLECTION_FIELDS, streamingConfig);

        Response.Builder builder = new Response.Builder();

        try {

            String isValidRelationship = streamingOperationsRepository.isValidRelationshipExists(streamingConfig);

            if (!isValidRelationship.equals(Constant.SUCCESS)) {
                return Utils.getFailedResponseStatus(builder, isValidRelationship);
            }

            if (streamingOperationsRepository.getStreamingConfigDetailsByName(streamingConfig.getName()) == null) {
                streamingOperationsRepository.saveStreamingConfig(streamingConfig);
                return Utils.getSuccessResponseWithData(builder, streamingConfig);
            } else
                return Utils.getFailedResponseStatus(builder, Constant.DUPLICATE_STREAMING_CONFIG_NAME);

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response updateStreamingConfig(StreamingConfig streamingConfig) {

        logger.debug("Method Name: [{}] Request:  [{}] ", URLEndPoints.GET_COLLECTION_FIELDS, streamingConfig);

        Response.Builder builder = new Response.Builder();

        try {

            String isValidRelationship = streamingOperationsRepository.isValidRelationshipExists(streamingConfig);

            if (!isValidRelationship.equals(Constant.SUCCESS)) {
                return Utils.getFailedResponseStatus(builder, isValidRelationship);
            }

            streamingOperationsRepository.updateStreamingConfig(streamingConfig);
            return Utils.getSuccessResponseWithData(builder, streamingConfig);

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response getStreamingConfigDetails(String id) {

        logger.debug("Method Name: [{}] Request:  [{}] ", URLEndPoints.GET_COLLECTION_FIELDS, id);

        Response.Builder builder = new Response.Builder();

        try {

            StreamingConfig streamingConfig = streamingOperationsRepository.getStreamingConfigDetails(id);

            if (streamingConfig != null)
                return Utils.getSuccessResponseWithData(builder, streamingConfig);
            else
                return Utils.getFailedResponseStatus(builder, "Unable to find Streaming Config with id " + id);

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response getStreamingConfigList() {

        logger.debug("Method Name: [{}] ", URLEndPoints.GET_COLLECTION_FIELDS);

        Response.Builder builder = new Response.Builder();

        try {

            List<StreamingConfig> streamingConfigList = streamingOperationsRepository.getStreamingConfigList();

            if (streamingConfigList != null)
                return Utils.getSuccessResponseWithData(builder, streamingConfigList);
            else
                return Utils.getFailedResponseStatus(builder, "Unable to find streaming configs.");

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response testJavascript(JavaScript script) {

        logger.debug("Method Name: [{}] Request:  [{}] ", URLEndPoints.GET_COLLECTION_FIELDS, script);

        Response.Builder builder = new Response.Builder();

        try {

            Object result = Utils.runJavascripCode(script);

            if (result != null)
                return Utils.getSuccessResponseWithData(builder, result);
            else
                return Utils.getFailedResponseStatus(builder, "Something went wrong.");

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder, ex.getMessage());
        }
    }

}
