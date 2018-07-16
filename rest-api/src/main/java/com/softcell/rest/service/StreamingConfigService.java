package com.softcell.rest.service;


import com.softcell.domains.StreamingConfig;
import com.softcell.domains.response.Response;

import com.softcell.persistance.StreamingOperationsRepository;
import com.softcell.rest.utils.Utils;
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

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    StreamingOperationsRepository streamingOperationsRepository;

    public Response getCollectionNames() {

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

        Response.Builder builder = new Response.Builder();

        try {

            streamingOperationsRepository.saveStreamingConfig(streamingConfig);
            return Utils.getSuccessResponseWithData(builder, streamingConfig);

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response updateStreamingConfig(StreamingConfig streamingConfig) {

        Response.Builder builder = new Response.Builder();

        try {



            streamingOperationsRepository.saveStreamingConfig(streamingConfig);
            return Utils.getSuccessResponseWithData(builder, streamingConfig);

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }

    public Response getStreamingConfigDetails(String id) {

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
}
