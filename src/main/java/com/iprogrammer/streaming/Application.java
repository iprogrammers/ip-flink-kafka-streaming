package com.iprogrammer.streaming;

import com.iprogrammer.streaming.streaming.flink.StreamingOperations;
import com.iprogrammer.streaming.model.Oplog;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {

        ApplicationContext context = SpringApplication.run(Application.class);
        StreamingOperations streamingOperations = context.getBean(StreamingOperations.class);
        streamingOperations.startConsumingOplog(context);
        streamingOperations.startStreamingOperation();
    }

    public static long getTimeStamp(Oplog document) {
        return document.getTs().getValue() >> 32;
    }

}
