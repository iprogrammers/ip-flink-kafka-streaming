package com.softcell.persistance.utils;

import com.softcell.domains.FieldConfig;
import com.softcell.domains.StreamingConfig;
import com.softcell.utils.Constant;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;

public class RepositoryHelper {

    private static final String FIELD_SEPERATOR = ".";

    private RepositoryHelper() {
    }

    public static int getNumberOfCollections(StreamingConfig streamingConfig) {

        int numberOfCollections = streamingConfig.getFields().size();

        if (streamingConfig.getFields() != null && streamingConfig.getFields().containsKey(Constant.DERIVED_FIELDS))
            numberOfCollections = numberOfCollections - 1;

        return numberOfCollections;
    }


    public static FieldConfig getPrimaryAndForeignKeysFromConfig(StreamingConfig streamingConfig, List<FieldConfig> foreignKeys) {

        FieldConfig primaryCollectionField = null;

        for (Map.Entry<String, List<FieldConfig>> entry : streamingConfig.getFields().entrySet()) {

            if (!entry.getKey().equals(Constant.DERIVED_FIELDS)) {

                Optional<FieldConfig> primaryCollectionOptional = entry.getValue().stream().filter(FieldConfig::isPrimaryKey).findFirst();

                Optional<FieldConfig> foreignCollectionOptional = entry.getValue().stream().filter(FieldConfig::isForeignKey).findFirst();

                if (primaryCollectionOptional.isPresent())
                    primaryCollectionField = primaryCollectionOptional.get();
                else if (foreignCollectionOptional.isPresent())
                    foreignKeys.add(foreignCollectionOptional.get());
            }
        }

        return primaryCollectionField;
    }

    public static void getKeyValuesFromDocument(String prefix, Document dbObject, Map<String, String> keyValues) {

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

                                if (i == 0 && StringUtils.isNotEmpty(prefix) && !prefix.endsWith(FIELD_SEPERATOR)) {
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


    private static String getType(Object value) {
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

}


