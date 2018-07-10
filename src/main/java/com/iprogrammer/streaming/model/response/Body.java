package com.iprogrammer.streaming.model.response;

import java.io.Serializable;

public class Body<T> implements Serializable {
    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    private T payload;
}
