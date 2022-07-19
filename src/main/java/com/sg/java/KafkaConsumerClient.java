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
        log.info("重庆客户kafka组件kerberos认证文件配置");
        SecurityPrepare.cqEcsKerberosLogin();
        log.info("读取kafka消费者配置文件");
        Properties prop = PropertiesUtil.createAndLoadPropertiesFromFileOrResource(null, ResourcePath.kafka_consumer_properties);
        log.info("kafka消费者配置文件内容："+prop.toString());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop)) {
            log.info("创建一个consumer实例对象");
            consumer.subscribe(Collections.singletonList("cms_volt_curve"));
            log.info("订阅主题：{}","cms_volt_curve");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            log.info("单次消息拉取最大等待时间：5s");
            if (!records.isEmpty()) {
                records.forEach(cr -> log.info("消费者客户端消费消息 " +
                                           "topic:" + cr.topic() + "\t" +
                                           "partition:" + cr.partition() + "\t" +
                                           "offset:" + cr.offset() + "\t" +
                                           "value:" + cr.value()));

            }
        }
        log.info("consumer关闭");
    }

}
