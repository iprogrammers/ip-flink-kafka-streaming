package com.iprogrammer.streaming.model.response;

import lombok.Data;

import java.io.Serializable;

@Data
public class Errors implements Serializable {
    int errorCode;
    String message;
}
