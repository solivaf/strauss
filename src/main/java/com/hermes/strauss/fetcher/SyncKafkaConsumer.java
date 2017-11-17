package com.hermes.strauss.fetcher;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SyncKafkaConsumer implements SyncConsumer<String, String> {

    private static final Logger log = LoggerFactory.getLogger(SyncKafkaConsumer.class);
    private final List<String> topic;
    private Consumer<String, String> consumer;
    private Boolean processPartitionsAsync = Boolean.TRUE;
    private Boolean processRecordsAsync = Boolean.TRUE;
    private Boolean processRecordsAndPartitionsAsync = Boolean.TRUE;

    public SyncKafkaConsumer(Consumer<String, String> consumer, Properties consumerConfig) {
        this.consumer = consumer;
        this.topic = Arrays.asList(consumerConfig.get("topics").toString().split(","));
        this.processRecordsAndPartitionsAsync = processPartitionsAsync && processRecordsAsync;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topic);

            final ConsumerRecords<String, String> records = getRecords(consumer);
            final Map<TopicPartition, OffsetAndMetadata> lastRecords = processRecords(records);
            commitRecord(consumer, lastRecords);
        } catch (Exception e) {
            log.error("Error processing messages from Kafka. message - {}", e.getMessage(), e);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> processRecords(ConsumerRecords<String, String> records) {
        final HashMap<TopicPartition, OffsetAndMetadata> lastRecords = new HashMap<>();

        if (processRecordsAndPartitionsAsync) {
            records.partitions().parallelStream()
                    .forEach(topicPartition -> records.records(topicPartition).parallelStream()
                            .forEach(cr -> lastOffset(lastRecords, topicPartition, cr.offset())));
        } else if (processPartitionsAsync) {
            records.partitions().parallelStream()
                    .forEach(topicPartition -> records.records(topicPartition)
                            .forEach(cr -> lastOffset(lastRecords, topicPartition, cr.offset())));
        } else {
            records.partitions().forEach(topicPartition -> records.records(topicPartition)
                    .forEach(cr -> lastOffset(lastRecords, topicPartition, cr.offset())));
        }

        return lastRecords;
    }

    @Override
    public void commitRecord(Consumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> lastRecord) throws Exception {
        consumer.commitSync(lastRecord);
    }
}
