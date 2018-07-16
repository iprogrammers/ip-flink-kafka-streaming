package com.softcell.streaming.kafka;


import com.softcell.streaming.utils.ZkStringSerializer;
import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;

import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Map;
import scala.collection.Seq;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

public class KafkaUtil implements Closeable {
    private final static int CONSUMER_TIMEOUT = 100000;
    private final static int CONSUMER_BUFFER_SIZE = 64 * 1024;
    private final static String CONSUMER_CLIENT_ID = "leaderLookup";
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass().getName());
    private final ZkClient zkClient;
    private final SimpleConsumer simpleConsumer;

    public KafkaUtil(String zokeeperCluster, String broker, int brokerPort, int connectionTimeout, int sessionTimeout) {
        this.zkClient = new ZkClient(zokeeperCluster, sessionTimeout, connectionTimeout, new ZkStringSerializer());
        this.simpleConsumer = new SimpleConsumer(broker, brokerPort, CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE,
                CONSUMER_CLIENT_ID);
    }

    public void createTopicIfNotExist(String topic, int replicationFactor, int partitions) {
        ZkUtils utils = ZkUtils.apply(zkClient, false);
        if (!AdminUtils.topicExists(utils, topic)) {
            createOrUpdateTopic(topic, replicationFactor, partitions);
        } else {
            if (LOGGER.isInfoEnabled())
                LOGGER.info("Topic " + topic + " already exists");
        }
    }

    public void createOrUpdateTopic(String topic, int replicationFactor, int partitions) {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Creating topic " + topic + " with replication " + replicationFactor + " and " + partitions
                    + " partitions");

        //Seq<Object> brokerList = ZkUtils.getSortedBrokerList(zkClient);
        ZkUtils utils = ZkUtils.apply(zkClient, false);

        Seq<BrokerMetadata> brokerMeta = AdminUtils.getBrokerMetadatas(utils, AdminUtils.getBrokerMetadatas$default$2(), AdminUtils.getBrokerMetadatas$default$3());

        Map<Object, Seq<Object>> partitionReplicaAssignment = AdminUtils.assignReplicasToBrokers(brokerMeta,
                partitions, replicationFactor, AdminUtils.assignReplicasToBrokers$default$4(),
                AdminUtils.assignReplicasToBrokers$default$5());


        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(utils, topic, partitionReplicaAssignment,
                AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$4(),
                /*update = */true);
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Topic " + topic + " created");
    }

    public Integer getNumPartitionsForTopic(String topic) {
        TopicMetadataRequest topicRequest = new TopicMetadataRequest(Arrays.asList(topic));
        TopicMetadataResponse topicResponse = simpleConsumer.send(topicRequest);
        for (TopicMetadata topicMetadata : topicResponse.topicsMetadata()) {
            if (topic.equals(topicMetadata.topic())) {
                int partitionSize = topicMetadata.partitionsMetadata().size();
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Partition size found (" + partitionSize + ") for " + topic + " topic");
                return partitionSize;
            }
        }
        LOGGER.warn("Metadata info not found!. TOPIC " + topic);
        return null;
    }

    public void deleteTopics() {
        //zkClient.deleteRecursive("/brokers/topics");
    }

    public void close() throws IOException {
     //   zkClient.close();

//        zkClient.close();
        simpleConsumer.close();
    }
}