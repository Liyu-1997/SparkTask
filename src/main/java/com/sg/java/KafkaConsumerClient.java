package com.sg.java;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerClient {

    public static void main(String[] args) {

        Properties prop = PropertiesUtil.createAndLoadPropertiesFromFileOrResource(null, ResourcePath.kafka_consumer_properties);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop)) {
            consumer.subscribe(Collections.singletonList("topic1,topic2"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofHours(1));
                if (!records.isEmpty()) {
                    records.forEach(item -> {
                        System.out.println(item);
                    });
                } else break;
            }
        }
    }

}
