package com.hermes.strauss;

import com.hermes.strauss.fetcher.SimpleKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class SimpleKafkaConsumerInit implements CommandLineRunner {

    private ExecutorService executorService;
    private Properties consumerConfig;

    public SimpleKafkaConsumerInit(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    @Override
    public void run(String... strings) throws Exception {
        final Integer consumers = (Integer) consumerConfig.get("consumers");
        executorService = Executors.newFixedThreadPool(consumers);

        for (int i = 0; i < consumers; i++) {
            executorService.execute(new SimpleKafkaConsumer(new KafkaConsumer<>(consumerConfig), consumerConfig));
        }
    }


}
