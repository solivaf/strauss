package com.hermes.strauss.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConsumerConfig {

    public static final String TOPICS = "topics";
    public static final String CONSUMERS_THREAD = "consumers.thread";
    public static final String ASYNC_RECORDS = "async.records";
    public static final String ASYNC_PARTITIONS = "async.partitions";

    private String groupId;
    private String brokers;
    private String offsetResetStrategy;
    private Integer maxPollInterval;
    private Integer autoCommitInterval;
    private Integer heartBeatInterval;
    private Integer fetchMaxBytes;
    private Integer maxPollRecords;
    private Boolean autoCommitEnabled;
    private Boolean asyncPartitions;
    private Boolean asyncRecords;
    private Integer consumers;
    private String topics;

    @Bean
    public Properties consumerConfig() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommitEnabled);

        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartBeatInterval);
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        properties.put(TOPICS, topics);
        properties.put(CONSUMERS_THREAD, consumers);
        properties.put(ASYNC_RECORDS, asyncRecords);
        properties.put(ASYNC_PARTITIONS, asyncPartitions);
        return properties;
    }

    @Value("${strauss.kafka.consumer.group.id}")
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Value("${strauss.kafka.consumer.bootstrap.servers}")
    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    @Value("${strauss.kafka.consumer.auto.offset.reset}")
    public void setOffsetResetStrategy(String offsetResetStrategy) {
        this.offsetResetStrategy = offsetResetStrategy;
    }

    @Value("${strauss.kafka.consumer.max.poll.interval.ms}")
    public void setMaxPollInterval(Integer maxPollInterval) {
        this.maxPollInterval = maxPollInterval;
    }

    @Value("${strauss.kafka.consumer.heartbeat.interval.ms}")
    public void setHeartBeatInterval(Integer heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
    }

    @Value("${strauss.kafka.consumer.auto.commit.interval.ms}")
    public void setAutoCommitInterval(Integer autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    @Value("${strauss.kafka.consumer.fetch.max.bytes}")
    public void setFetchMaxBytes(Integer fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
    }

    @Value("${strauss.kafka.consumer.max.poll.records}")
    public void setMaxPollRecords(Integer maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    @Value("${strauss.kafka.consumer.enable.auto.commit}")
    public void setAutoCommitEnabled(Boolean autoCommitEnabled) {
        this.autoCommitEnabled = autoCommitEnabled;
    }

    @Value("${strauss.kafka.consumer.async.partitions}")
    public void setAsyncPartitions(Boolean asyncPartitions) {
        this.asyncPartitions = asyncPartitions;
    }

    @Value("${strauss.kafka.consumer.async.records}")
    public void setAsyncRecords(Boolean asyncRecords) {
        this.asyncRecords = asyncRecords;
    }

    @Value("${strauss.kafka.consumer.consumers}")
    public void setConsumers(Integer consumers) {
        this.consumers = consumers;
    }

    @Value("${strauss.kafka.consumer.topics}")
    public void setTopics(String topics) {
        this.topics = topics;
    }

}
