package com.softcell.utils;

public class URLEndPoints {

    private URLEndPoints() {
        throw new IllegalStateException("URLEndPoints class");
    }

    public static final String GET_COLLECTION_NAMES="get-collection-names";
    public static final String GET_COLLECTION_FIELDS="get-collection-fields";
    public static final String CREATE_STREAMING_CONFIG ="create-streaming-config";
    public static final String GET_STREAMING_CONFIG_DETAILS ="get-streaming-config-details";
    public static final String GET_STREAMING_CONFIG_LIST ="get-streaming-config-list";
    public static final String UPDATE_STREAMING_CONFIG ="update-streaming-config";
    public static final String UPDATE_STREAMING_CONFIG_STATUS ="update-streaming-config-status";
    public static final String TEST_JAVASCRIPT ="test-javascript";

}
