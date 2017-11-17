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

public class SimpleSyncKafkaConsumer implements SyncFetcher<String, String> {

    private static final Logger log = LoggerFactory.getLogger(SimpleSyncKafkaConsumer.class);
    private final List<String> topic;
    private Consumer<String, String> consumer;

    public SimpleSyncKafkaConsumer(Consumer<String, String> consumer, Properties consumerConfig) {
        this.consumer = consumer;
        this.topic = Arrays.asList(consumerConfig.get("topics").toString().split(","));
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topic);
            log.info("Kafka Consumer subscribed at topics {}", topic);
            final ConsumerRecords<String, String> consumerRecords = this.getRecords(consumer);
            final Map<TopicPartition, OffsetAndMetadata> lastRecords = this.processRecords(consumerRecords);

            this.commitRecord(consumer, lastRecords);
        } catch (Exception e) {
            log.error("Error processing messages from Kafka. message - {}", e.getMessage(), e);
        }
    }

    @Override
    public ConsumerRecords<String, String> getRecords(Consumer<String, String> consumer) {
        final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        log.info("Poll {} records", records.count());
        return records;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> processRecords(ConsumerRecords<String, String> records) {
        final HashMap<TopicPartition, OffsetAndMetadata> lastRecords = new HashMap<>();
        records.forEach(cr -> records.partitions()
                .forEach(topicPartition -> records.records(topicPartition)
                        .forEach(record -> {
                            log.info("Processing message {}", record.value());
                            lastRecords.put(topicPartition, new OffsetAndMetadata(record.offset() + 1, null));
                        })));

        return lastRecords;
    }

    @Override
    public void commitRecord(Consumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> lastRecord) throws Exception {
        consumer.commitSync(lastRecord);
    }

    //    @Override
//    public void commitRecords(Consumer<String, String> consumer, ConsumerRecord<String, String> record) throws Exception {
//        consumer.commitSync();
//        if (records.isEmpty()) {
//            log.warn("No message returned from kafka");
//        } else if (asyncPartitions) {
//            long start = System.currentTimeMillis();
//            final List<List<ConsumerRecord<String, String>>> predicateStream = records.partitions().parallelStream()
//                    .map(records::records).collect(Collectors.toList());
//            if (asyncRecords) {
//                predicateStream.parallelStream().forEach(System.out::println);
//                log.info("FINISHED ASYNC PARTITIONS AND RECORDS = " + (System.currentTimeMillis() - start+ "ms"));
//            } else {
//                predicateStream.forEach(System.out::println);
//                log.info("FINISHED ASYNC PARTITIONS = " + (System.currentTimeMillis() - start+ "ms"));
//            }
//        } else if (asyncRecords) {
//            long start = System.currentTimeMillis();
//            records.partitions()
//                    .forEach(topicPartition -> records.records(topicPartition)
//                            .parallelStream().forEach(System.out::println));
//            log.info("FINISHED ASYNC RECORDS = " + (System.currentTimeMillis() - start) + "ms");
//        }
//    }
}
