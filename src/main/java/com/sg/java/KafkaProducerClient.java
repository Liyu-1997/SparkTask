package com.sg.java;

import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 此类用来创建kafka生产者客户端实例，仅为模拟生产者客户端，数据随机生产(kafka消费者客户端主要体现在scala编写的spark应用程序中)
 */
@Data
public class KafkaProducerClient {

    //kafka配置
    private final Properties                  prop;
    //kafka生产者
    private final KafkaProducer<Long, String> producer;
    //生产线程
    private       Thread                      produceThread = null;

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerClient.class);

    /**
     * 默认连接kafka服务端配置
     *
     * @return 配置
     */
    public static Properties defaultProp() throws Exception {
        Properties prop = new Properties();
        String kafkaProducerPropertiesPath = "kafka-producer.properties";
        try (InputStream is = ClassLoader.getSystemResourceAsStream(kafkaProducerPropertiesPath)) {
            if (is != null) {
                log.info("已读取kafka消费者客户端配置文件:" + kafkaProducerPropertiesPath);
                prop.load(is);
                is.close();
            } else {
                //kafka brokers地址（即kafka服务端实例地址）
                prop.setProperty("bootstrap.servers", "tcp://server.natappfree.cc:34508");
                //容错
                //消息发送失败后的重试次数，缺省 Integer.MAX_VALUE。默认情况下，生产者在每次重试之间等待 100ms，可以通过参数 retry.backoff.ms 参数来改变这个时间间隔。
                prop.setProperty("retries", "2");
                //这意味着领导者将等待全套同步副本确认记录。这保证了只要至少有一个同步副本保持活动状态，记录就不会丢失。这是最有力的保证。这相当于acks=-1设置。
                prop.setProperty("acks", "-1");

                //批处理：满足一个都会推送消息
                //当多个消息被发送同一个分区时，生产者会把它们放在同一个批次里。该参数指定了一个批次可以使用的内存大小，按照字节数计算。当批次内存 被填满后，批次里的所有消息会被发送出去。但是生产者不一定都会等到批次被填满才发送，半满甚至只包含一个消息的批次也有可能被发送。缺省 16384(16k) ，如果一条消息超过了批次的大小，会写不进去。
                prop.setProperty("batch.size", "128"); //达到指定字节
                //指定了生产者在发送批次前等待更多消息加入批次的时间, 缺省0 50ms 0就是不按批次走，有就发。
                prop.setProperty("linger.ms", "100"); //达到指定时间
                //消息键值的序列化
                prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            }
        }
        return prop;
    }

    public KafkaProducerClient(Properties prop) throws Exception {
        if (prop == null) {
            prop = defaultProp();
        }
        this.prop     = prop;
        this.producer = new KafkaProducer<>(prop);
    }

    /**
     * 阻塞监听控制台从输入读取消息发送
     *
     * @param topic 消息主题
     * @throws Exception io异常
     */
    public void startByREPL(String topic) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String value = reader.readLine();
            //exit退出
            if (value.equalsIgnoreCase("exit")) {
                break;
            }
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, null, value);
            RecordMetadata rmd = producer.send(record).get();
            System.out.println(rmd.topic() + "\t" + rmd.partition() + "\t" + rmd.offset() + "\t" + value);
        }
        reader.close();
        producer.close();
    }

    /**
     * @param topic        消息主题
     * @param intervalTime 发送消息间隔 单位毫秒ms
     * @param msgGenerator 消息生成器
     */
    public void start(String topic, long intervalTime, Supplier<String> msgGenerator) {
        produceThread = new Thread(() -> {
            while (true) {
                try {
                    String value = msgGenerator.get();
                    ProducerRecord<Long, String> record = new ProducerRecord<>(topic, value);
                    RecordMetadata rmd = producer.send(record).get();
                    System.out.println("生产者客户端生产消息 topic:" + rmd.topic() + "\tpartition:" + rmd.partition() + "\toffset:" + rmd.offset() + "\tvalue:" + value);
                    TimeUnit.MILLISECONDS.sleep(intervalTime);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        produceThread.start();
    }

    /**
     * 停止随机发送信息消息
     */
    public void close() {
        if (produceThread != null && produceThread.isAlive()) {
            produceThread.interrupt();
        }
        producer.close();
    }

}
