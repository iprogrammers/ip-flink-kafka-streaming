package com.softcell.streaming;


import com.softcell.domains.Oplog;
import com.softcell.streaming.flink.StreamingOperations;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

import java.util.Arrays;


@SpringBootApplication
@ComponentScan(basePackages = {"com.softcell"})
@EnableAutoConfiguration
@EnableMongoAuditing
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

    @Bean
    public CorsFilter corsFilter() {

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();

        CorsConfiguration config = new CorsConfiguration();

        config.setAllowedMethods(Arrays.asList("GET", "POST", "OPTIONS", "PUT", "PATCH", "DELETE"));

        config.applyPermitDefaultValues();

        source.registerCorsConfiguration("/**", config);

        return new CorsFilter(source);

    }


}
