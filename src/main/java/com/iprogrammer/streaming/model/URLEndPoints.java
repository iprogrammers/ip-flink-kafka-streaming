package com.iprogrammer.streaming.model;

public class URLEndPoints {

    private URLEndPoints() {
        throw new IllegalStateException("URLEndPoints class");
    }

    public static final String GET_COLLECTION_NAMES="get-collection-names";
    public static final String GET_COLLECTION_FIELDS="get-collection-fields";
    public static final String CREATE_STREAMING_CONFIG="create-streaming-config";
}
