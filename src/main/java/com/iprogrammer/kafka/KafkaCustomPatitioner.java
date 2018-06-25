package com.iprogrammer.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class KafkaCustomPatitioner implements Partitioner {

    private static final int MESSAGES_PER_KAFKA_PARTITION = 500;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {

        int randomPartition = 0;

        try {
            Oplog oplog = new ObjectMapper().readValue(value.toString(), Oplog.class);

            if (oplog.getO().get("SNO") != null) {

                int sno = Integer.parseInt(oplog.getO().getString("SNO"));

                if (sno <= MESSAGES_PER_KAFKA_PARTITION) {
                    return 0;
                } else
                    return sno / 500;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//         randomPartition = new Random().nextInt(3);
        return randomPartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
