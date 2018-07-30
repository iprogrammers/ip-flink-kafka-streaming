package com.softcell.rest.utils;

import com.softcell.domains.FieldConfig;
import com.softcell.domains.JavaScript;
import com.softcell.domains.StreamingConfig;
import com.softcell.domains.response.Error;
import com.softcell.domains.response.Payload;
import com.softcell.domains.response.Response;
import com.softcell.domains.response.Status;
import com.softcell.persistance.helper.RepositoryHelper;
import com.softcell.rest.service.StreamingConfigService;
import com.softcell.utils.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.util.CollectionUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    private static final String COLLECTION_SEPERATOR = "##";

    private Utils() {
    }

    public static Response getFailedResponseStatus(Response.Builder builder) {
        builder.status(new Status.Builder().statusValue(HttpStatus.FAILED_DEPENDENCY.name()).build());
        builder.error(new Error.Builder().message(Constant.TECHNICAL_ERROR).build());
        return builder.build();
    }

    public static Response getFailedResponseStatus(Response.Builder builder, String message) {
        builder.status(new Status.Builder().statusValue(HttpStatus.FAILED_DEPENDENCY.name())
                .statusCode(HttpStatus.INTERNAL_SERVER_ERROR.value()).build());
        builder.error(new Error.Builder().message(message).build());

        return builder.build();
    }

    public static Response getSuccessResponseWithData(Response.Builder builder, Object obj) {

        builder.payload(new Payload<>(obj));
        builder.status(new Status.Builder().statusValue(HttpStatus.OK.name()).build());
        return builder.build();
    }

    public static Object runJavascripCode(JavaScript javaScript) {

        try {
            ScriptEngineManager manager = new ScriptEngineManager();

            ScriptEngine engine = manager.getEngineByName("JavaScript");

            // JavaScript code in a String. This code defines a script object 'obj'
            // with one method called 'helo'.
            // evaluate script
            engine.eval(javaScript.getScript());

            // javax.script.Invocable is an optional interface.
            // Check whether your script engine implements or not!
            // Note that the JavaScript engine implements Invocable interface.
            Invocable inv = (Invocable) engine;

            Object result = inv.invokeFunction(javaScript.getMethodName(), javaScript.getParams());

            LOGGER.debug("result{}", result);

            return result;

        } catch (ScriptException | NoSuchMethodException ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);

        }

        return null;
    }

    public static File getMetaFile() {
        return new File(Constant.META_DIRECTORY + File.separator + Constant.META_FILENAME);
    }

    public static void updateStreamingConfigMeta(StreamingConfig streamingConfig) {

        List<FieldConfig> foreignKeys;

        int numberOfCollections = RepositoryHelper.getNumberOfCollections(streamingConfig);

        if (numberOfCollections > 1) {
            foreignKeys = new ArrayList<>();
            FieldConfig primaryCollectionField = RepositoryHelper.getPrimaryAndForeignKeysFromConfig(streamingConfig, foreignKeys);
            String collectionName = getStreamingMetaName(primaryCollectionField, foreignKeys);
            if (primaryCollectionField != null && !CollectionUtils.isEmpty(foreignKeys))
                getMetaFromKeys(collectionName, foreignKeys, primaryCollectionField, StreamingConfigService.STREAMING_CONFIG_META);
        }

    }

    private static Map<String, Map> getMetaFromKeys(String destCollectionName, List<FieldConfig> foreignKeys, FieldConfig primaryKeyField, Map<String, Map> metadata) {

        Map parentMeta = new HashMap();

        Map meta = new HashMap();
        meta.put(Constant.COLLECTION_NAME, primaryKeyField.getCollectionName());
        meta.put(Constant.PRIMARY_KEY, primaryKeyField.getActualParameter());

        parentMeta.put(Constant.PARENT_COLLECTION, meta);

        List<Map> foreignMetaList = new ArrayList<>();
        for (FieldConfig fieldConfig : foreignKeys) {
            meta = new HashMap();
            meta.put(Constant.COLLECTION_NAME, fieldConfig.getCollectionName());
            meta.put(Constant.FOREIGN_KEY, fieldConfig.getActualParameter());
            foreignMetaList.add(meta);
        }

        parentMeta.put(Constant.CHILD_COLLECTIONS, foreignMetaList);

        metadata.put(destCollectionName, parentMeta);

        return metadata;
    }

    private static String getStreamingMetaName(FieldConfig primaryCollectionField, List<FieldConfig> foreignKeys) {

        StringBuilder stringBuilder = new StringBuilder();
        appendPrefixAndSuffix(stringBuilder, primaryCollectionField.getCollectionName());

        for (FieldConfig fieldConfig : foreignKeys) {
            appendPrefixAndSuffix(stringBuilder, fieldConfig.getCollectionName());
        }
        return stringBuilder.toString();
    }

    private static void appendPrefixAndSuffix(StringBuilder stringBuilder, String collectionName) {
        stringBuilder.append(COLLECTION_SEPERATOR);
        stringBuilder.append(collectionName);
        stringBuilder.append(COLLECTION_SEPERATOR);
    }
}
