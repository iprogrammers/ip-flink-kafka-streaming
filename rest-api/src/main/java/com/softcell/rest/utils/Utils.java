package com.softcell.rest.utils;

import com.softcell.domains.response.Error;
import com.softcell.domains.response.Payload;
import com.softcell.domains.response.Response;
import com.softcell.domains.response.Status;
import com.softcell.utils.Constant;
import org.springframework.http.HttpStatus;

public class Utils {

    public static Response getFailedResponseStatus(Response.Builder builder) {
        builder.status(new Status.Builder().statusValue(HttpStatus.FAILED_DEPENDENCY.name()).build());
        builder.error(new Error.Builder().message(Constant.TECHNICAL_ERROR).build());
        return builder.build();
    }

    public static Response getFailedResponseStatus(Response.Builder builder, String message) {
        builder.status(new Status.Builder().statusValue(HttpStatus.FAILED_DEPENDENCY.name())
                .statusCode(HttpStatus.INTERNAL_SERVER_ERROR.value()).build());
        builder.error(new Error.Builder().message(message).build());

        return builder.build();
    }

    public static Response getSuccessResponseWithData(Response.Builder builder, Object obj) {

        builder.payload(new Payload<>(obj));
        builder.status(new Status.Builder().statusValue(HttpStatus.OK.name()).build());
        return builder.build();
    }

}
