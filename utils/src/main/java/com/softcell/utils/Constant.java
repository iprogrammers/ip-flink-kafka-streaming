package com.softcell.utils;

public class Constant {

    private Constant() {
        throw new IllegalStateException("Constant class");
    }

    public static final String LOAN_APPLICATION_ID = "loanApplicationId";
    public static final String TECHNICAL_ERROR = "TECHINCAL ERROR OCCURED PLEASE CONTACT TO SERVICE PROVIDER.";
    public static final String DUPLICATE_STREAMING_CONFIG_NAME = "Please create config with another name.";
    public static final String PRIMARY_KEY_NOT_FOUND_EXCEPTION = "Primary key not found";
    public static final String FOREIGN_KEY_NOT_FOUND_EXCEPTION = "Please ensure if any foreign key is missing";
    public static final String INVALID_RELATIONSHIP_EXCEPTION = "Invalid relationship found between given collections";
    public static final String STREAMING_CONFIG_NOT_FOUND="Unable to find Streaming Config with id ";
    public static final String STREAMING = "streaming";
    //Data types
    public static final String STRING = "String";
    public static final String INT = "Int";
    public static final String BOOLEAN = "Boolean";
    public static final String DATE = "Date";
    public static final String OBJECT_ID = "ObjectId";
    public static final String OBJECT = "Object";

    public static final String NAME = "name";
    public static final String SUCCESS = "success";
    public static final String DERIVED_FIELDS = "derivedFields";

    public static final String COLLECTION_NAME = "collectionName";
    public static final String FOREIGN_KEY = "foreignKey";
    public static final String PRIMARY_KEY = "primaryKey";
    public static final String PARENT_COLLECTION = "parentCollection";
    public static final String CHILD_COLLECTIONS = "childCollections";
    public static final String META_FILENAME = "join_meta.json";
    public static final String META_DIRECTORY = "metadata";

    public static final String LOG_REQUEST_FORMAT = "Method Name: [{}] Request:  [{}]";
    public static final int DEFAULT_LIMIT = 100;

    public static final boolean STATUS_ENABLED=true;
    public static final boolean STATUS_DISABLED=false;

    public static final String JOB_CREATED="created";
    public static final String JOB_IN_SYNC="in-sync";
    public static final String JOB_COMPLETED="completed";
    public static final String JOB_DISABLED="disabled";
    public static final String JOB_FAILED="failed";

    public static final String OPLOG_TIMESTAMP_COLLECTION="oplogTimestamp";
    public static final String OPLOG_DB="local";
    public static final String OPLOG_COLLECTION_NAME="oplog.rs";
    public static final String IS_DERIVED_COLLECTION="isDerivedCollection";
    public static final String DERIVED_CLASS_NAME="derivedClassName";

}
