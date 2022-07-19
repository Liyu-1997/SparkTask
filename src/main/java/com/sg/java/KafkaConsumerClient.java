package com.sg.java;

import com.sg.java.security.SecurityPrepare;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerClient.class);

    public static void main(String[] args) {
        SecurityPrepare.kerberosLogin();
        Properties prop = PropertiesUtil.createAndLoadPropertiesFromFileOrResource(null, ResourcePath.kafka_consumer_properties);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop)) {
            consumer.subscribe(Collections.singletonList("cms_volt_curve"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (!records.isEmpty()) {
                records.forEach(cr -> {
                    log.info("消费者客户端消费消息 " +
                             "topic:" + cr.topic() + "\t" +
                             "partition:" + cr.partition() + "\t" +
                             "offset:" + cr.offset() + "\t" +
                             "value:" + cr.value());

                });

            }
        }
    }

}
