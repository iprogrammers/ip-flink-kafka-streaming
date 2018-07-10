package com.iprogrammer.streaming.service;

import com.iprogrammer.streaming.model.response.Response;
import com.iprogrammer.streaming.repository.StreamingOperationsRepository;
import com.iprogrammer.streaming.utils.Utils;
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
                List collectionFields = streamingOperationsRepository.getCollectionFields(collectionName);

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

            List collectionFields = streamingOperationsRepository.getCollectionFields(collectionName);

            if (collectionFields != null)
                return Utils.getSuccessResponseWithData(builder, collectionFields);

            return Utils.getFailedResponseStatus(builder);

        } catch (Exception ex) {
            logger.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
            return Utils.getFailedResponseStatus(builder);
        }
    }
}
