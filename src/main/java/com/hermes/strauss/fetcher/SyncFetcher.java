package com.hermes.strauss.fetcher;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

interface SyncFetcher<K, V> extends Runnable {
    ConsumerRecords<K, V> getRecords(Consumer<K, V> consumer);
    Map<TopicPartition, OffsetAndMetadata> processRecords(ConsumerRecords<K, V> records);
    void commitRecord(Consumer<K, V> consumer, Map<TopicPartition, OffsetAndMetadata> lastRecord) throws Exception;
}
