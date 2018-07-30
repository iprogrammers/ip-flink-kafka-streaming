package com.softcell.utils;

public class FieldName {

    private FieldName() {
        throw new IllegalStateException("Constant class");
    }

    public static final String UPDATED_AT = "updatedAt";
    public static final String DOCUMENT_ID = "_id";
    public static final String SCHEDULE_AT = "scheduleAt";
    public static final String NAME="name";
    public static final String STATUS = "status";
    public static final String JOB_STATUS = "jobStatus";
}
