package com.softcell.streaming.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.softcell.domains.Oplog;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.Map;

public class KafkaCustomPatitioner implements Partitioner {

    private static final int MESSAGES_PER_KAFKA_PARTITION = 500;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCustomPatitioner.class);

    public static Long getNumericReferenceNumber(String str) {

        long result = 0;

        for (int i = 0; i < str.length(); i++) {

            char ch = str.charAt(i);

            if (Character.isLetter(ch)) {
                char initialCharacter = Character.isUpperCase(ch) ? 'A' : 'a';
                result = result + (ch - initialCharacter + 1);
            } else result = result + ch;
        }

        return result;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {

        int randomPartition = 0;

        try {

            Oplog oplog = new ObjectMapper().readValue(value.toString(), Oplog.class);

            if (oplog.getO() != null && oplog.getO().get("SNO") != null) {

                int sno = Integer.parseInt(oplog.getO().getString("SNO"));

                if (sno <= MESSAGES_PER_KAFKA_PARTITION) {
                    return 0;
                } else
                    return sno / 500;
            } else if (oplog.getO() != null && oplog.getO().get("_id") != null) {

                long number = getNumericReferenceNumber(oplog.getO().get("_id").toString());
                return (int) number % 10;
            }
        } catch (IOException ex) {
            LOGGER.error(HttpStatus.FAILED_DEPENDENCY.name(), ex);
        }

        return randomPartition;
    }

    @Override
    public void close() {
        //require to close connections
    }

    @Override
    public void configure(Map<String, ?> map) {
        //require to configure connections
    }
}
