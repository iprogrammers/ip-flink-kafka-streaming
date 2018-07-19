package com.softcell.rest.utils;

import com.softcell.domains.JavaScript;
import com.softcell.domains.response.Error;
import com.softcell.domains.response.Payload;
import com.softcell.domains.response.Response;
import com.softcell.domains.response.Status;
import com.softcell.utils.Constant;
import org.springframework.http.HttpStatus;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

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

    public static Object runJavascripCode(JavaScript javaScript) {

        try {

            ScriptEngineManager manager = new ScriptEngineManager();

            ScriptEngine engine = manager.getEngineByName("JavaScript");

            // evaluate script
            engine.eval(javaScript.getScript());

            // javax.script.Invocable is an optional interface.
            // Check whether your script engine implements or not!
            // Note that the JavaScript engine implements Invocable interface.
            Invocable inv = (Invocable) engine;

            Object result = inv.invokeFunction(javaScript.getMethodName(), javaScript.getParams());

            System.out.println("result: " + result);

            return result;

        } catch (ScriptException e) {
            // Shouldn't happen unless somebody breaks the script
            throw new RuntimeException(e);

        } catch (NoSuchMethodException e) {
            e.printStackTrace();

        }

        return null;
    }
}
