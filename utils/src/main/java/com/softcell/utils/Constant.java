package com.softcell.utils;

public class Constant {
    private Constant() {
        throw new IllegalStateException("Constant class");
    }

    public static final String LOAN_APPLICATION_ID="loanApplicationId";
    public static final String TECHNICAL_ERROR = "TECHINCAL ERROR OCCURED PLEASE CONTACT TO SERVICE PROVIDER.";
    public static final String DUPLICATE_STREAMING_CONFIG_NAME="Please create config with another name.";
    public static final String PRIMARY_KEY_NOT_FOUND_EXCEPTION="Primary key not found";
    public static final String FOREIGN_KEY_NOT_FOUND_EXCEPTION="Please ensure if any foreign key is missing";
    public static final String INVALID_RELATIONSHIP_EXCEPTION = "Invalid relationship found between given collections";
    public static final String REPORTS = "reports";

    //Data types
    public static final String STRING = "String";
    public static final String INT = "Int";
    public static final String BOOLEAN = "Boolean";
    public static final String DOUBLE = "Double";
    public static final String DATE = "Date";
    public static final String OBJECT_ID = "ObjectId";
    public static final String OBJECT = "Object";

    public static final String DOCUMENT_ID = "_id";
    public static final String NAME = "name";
    public static final String SUCCESS = "success";
    public static final String DERIVED_COLLECTION="derivedCollection";
    //Exception Messages




}
