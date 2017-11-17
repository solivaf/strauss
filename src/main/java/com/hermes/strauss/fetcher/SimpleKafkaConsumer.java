package com.hermes.strauss.fetcher;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleKafkaConsumer implements Fetcher {

    private static final Logger log = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    private final List<String> topic;
    private Boolean asyncPartitions;
    private Boolean asyncRecords;
    private Consumer<String, String> consumer;

    public SimpleKafkaConsumer(Consumer<String, String> consumer, Properties consumerConfig) {
        this.consumer = consumer;
        this.topic = Arrays.asList(consumerConfig.get("topics").toString().split(","));
        this.asyncPartitions = (Boolean) consumerConfig.get("asyncPartitions");
        this.asyncRecords = (Boolean) consumerConfig.get("asyncRecords");
    }

    @Override
    public void run() {
        consumer.subscribe(topic);
        final ConsumerRecords<String, String> consumerRecords = consumer.poll(Long.MAX_VALUE);
        if (consumerRecords.isEmpty()) {
            log.warn("No message returned from kafka");
        } else if (asyncPartitions) {
            long start = System.currentTimeMillis();
            final List<List<ConsumerRecord<String, String>>> predicateStream = consumerRecords.partitions().parallelStream()
                    .map(consumerRecords::records).collect(Collectors.toList());
            if (asyncRecords) {
                predicateStream.parallelStream().forEach(System.out::println);
                log.info("FINISHED ASYNC PARTITIONS AND RECORDS = " + (System.currentTimeMillis() - start+ "ms"));
            } else {
                predicateStream.forEach(System.out::println);
                log.info("FINISHED ASYNC PARTITIONS = " + (System.currentTimeMillis() - start+ "ms"));
            }
        } else if (asyncRecords) {
            long start = System.currentTimeMillis();
            consumerRecords.partitions()
                    .forEach(topicPartition -> consumerRecords.records(topicPartition)
                            .parallelStream().forEach(System.out::println));
            log.info("FINISHED ASYNC RECORDS = " + (System.currentTimeMillis() - start) + "ms");
        }
    }

}
