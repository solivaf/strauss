package com.hermes.strauss.fetcher;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

interface SyncConsumer<K, V> extends com.hermes.strauss.fetcher.Consumer, Runnable {

    default ConsumerRecords<K, V> getRecords(Consumer<K, V> consumer) {
        return consumer.poll(Long.MAX_VALUE);
    }

    default void lastOffset(HashMap<TopicPartition, OffsetAndMetadata> lastRecords, TopicPartition topicPartition, long offset) {
        lastRecords.put(topicPartition, new OffsetAndMetadata(offset + 1, null));
    }

    Map<TopicPartition, OffsetAndMetadata> processRecords(ConsumerRecords<K, V> records);

    void commitRecord(Consumer<K, V> consumer, Map<TopicPartition, OffsetAndMetadata> lastRecord) throws Exception;
}
