package com.iprogrammer.kafka;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * The serialization schema for the {@link KafkaEvent} type. This class defines how to transform a
 * Kafka record's bytes to a {@link KafkaEvent}, and vice-versa.
 */
public class KafkaEventSchema implements DeserializationSchema<Document>, SerializationSchema<Document> {

    private static final long serialVersionUID = 6154188370181669758L;

    private final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    @Autowired
    ObjectMapper objectMapper;

    @Override
    public byte[] serialize(Document event) {
        try {
            return new ObjectMapper().writeValueAsString(event).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Document deserialize(byte[] message) throws IOException {

        String strMessage = new String(message);

        try {
            return new ObjectMapper().readValue(strMessage, Document.class);
        } catch (IOException ex) {
            LOGGER.error("Exception in deserialize", ex);
        }

        return null;
    }

    @Override
    public boolean isEndOfStream(Document nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Document> getProducedType() {
        return TypeInformation.of(Document.class);
    }
}
