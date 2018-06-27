package com.iprogrammer.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class KafkaEventOplogSchema  implements KeyedDeserializationSchema<Oplog>, SerializationSchema<Oplog> {

    private static final long serialVersionUID = 6154188370181669758L;

    private final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    @Autowired
    ObjectMapper objectMapper;

    @Override
    public byte[] serialize(Oplog event) {
        return event.toString().getBytes();
    }

  /*  @Override
    public Oplog deserialize(byte[] message) throws IOException {

        String strMessage = new String(message);

        try {
            return new ObjectMapper().readValue(strMessage, Oplog.class);
        } catch (IOException ex) {
            LOGGER.error("Exception in deserialize", ex);
        }

        return null;
    }
*/

    @Override
    public Oplog deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {

        String strMessage = new String(message);

        try {
            Oplog oplog= new ObjectMapper().readValue(strMessage, Oplog.class);
            oplog.setPartitionId(partition);
            return oplog;
        } catch (IOException ex) {
            LOGGER.error("Exception in deserialize", ex);
        }

        return null;
    }

    @Override
    public boolean isEndOfStream(Oplog nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Oplog> getProducedType() {
        return TypeInformation.of(Oplog.class);
    }
}
