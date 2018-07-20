package com.softcell.rest.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.softcell.domains.StreamingConfig;
import com.softcell.persistance.StreamingOperationsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
public class StreamingCache {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    StreamingOperationsRepository streamingOperationsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @PostConstruct
    public void initializeStreamingMeta() {

        logger.debug("Method Name: [{}]", "initializeStreamingMeta");

        List<StreamingConfig> streamingConfigList = streamingOperationsRepository.getStreamingConfigList();

        if (streamingConfigList != null) {
            for (StreamingConfig streamingConfig : streamingConfigList) {
                Utils.updateStreamingConfigMeta(streamingConfig);
            }
        }

    }

}
