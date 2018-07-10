package com.iprogrammer.streaming.utils;

import com.iprogrammer.streaming.model.response.Payload;
import com.iprogrammer.streaming.model.response.Response;
import com.iprogrammer.streaming.model.response.Status;
import com.mongodb.client.MongoIterable;
import org.apache.commons.collections.IteratorUtils;
import org.springframework.http.HttpStatus;
import com.iprogrammer.streaming.model.response.Error;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

public class Utils {

    public static Response getFailedResponseStatus(Response.Builder builder) {
        builder.status(new Status.Builder().statusValue(HttpStatus.FAILED_DEPENDENCY.name()).build());
        builder.error(new Error.Builder().message(Constant.TECHNICAL_ERROR).build());
        return builder.build();
    }


    public static Response getSuccessResponseWithData(Response.Builder builder, Object obj) {

        builder.payload(new Payload<>(obj));
        builder.status(new Status.Builder().statusValue(HttpStatus.OK.name()).build());
        return builder.build();
    }

    public static Set getSettypeFromMongoIterable( MongoIterable<String> iterable){
        if (iterable != null)
            return new TreeSet<String>(IteratorUtils.toList(iterable.iterator()));
        else return Collections.emptySet();
    }
}
