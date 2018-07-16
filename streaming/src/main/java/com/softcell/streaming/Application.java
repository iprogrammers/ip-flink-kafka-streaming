package com.softcell.streaming;


import com.softcell.domains.Oplog;
import com.softcell.streaming.flink.StreamingOperations;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = {"com.softcell"})
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
