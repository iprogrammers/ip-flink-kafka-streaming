package com.softcell.domains;

public class MongoDBConstants {
    public static final String OPLOG_OPERATION = "op";
    public static final String OPLOG_OPERATION_NO_OP = "n";
    public static final String OPLOG_OPERATION_INSERT = "i";
    public static final String OPLOG_OPERATION_UPDATE = "u";
    public static final String OPLOG_OPERATION_DELETE = "d";
    public static final String OPLOG_NAME_SPACE = "ns";
    public static final String OPLOG_TIMESTAMP = "ts";
    public static final String OPLOG_ID = "h";
    private MongoDBConstants() {
        throw new IllegalStateException("MongoDBConstants class");
    }

}
