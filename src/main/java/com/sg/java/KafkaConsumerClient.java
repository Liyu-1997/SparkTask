package com.sg.java;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.sg.java.security.SecurityPrepare;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

import static com.sg.java.Constant.HBASE_NAMESPACE;

public class KafkaConsumerClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerClient.class);

    public static void main(String[] args) throws Exception {

        String cms_volt_curve = "cms_volt_curve";

        long start = System.currentTimeMillis();
        log.info("重庆客户kafka组件kerberos认证文件配置");
        SecurityPrepare.cqEcsKerberosLogin();
        log.info("读取kafka消费者配置文件");
        Properties prop = PropertiesUtil.createAndLoadPropertiesFromFileOrResource(null, ResourcePath.kafka_consumer_properties);
        log.info("kafka消费者配置文件内容：" + prop.toString());

        int interval = 30;
        log.info("创建3个consumer");
        log.info("kafka服务端地址：{}", prop.getProperty("bootstrap.servers"));
        log.info("消费者组：{}", prop.getProperty("group.id"));
        log.info("单次消息拉取最大等待时间：{}s", interval);
        log.info("单次消息拉取数据条数:{}", prop.getProperty("max.poll.records"));
        log.info("订阅主题：{}", cms_volt_curve);

        TableName hbaseTable_cms_volt_curve = TableName.valueOf(HBASE_NAMESPACE + ":" + cms_volt_curve);

        log.info("hbase建表：{}", cms_volt_curve);
        Connection createTableConn = HBaseUtil.getHBaseConn(PropertiesUtil.createPropertiesFromResource(ResourcePath.hbase_properties));
        Admin admin = createTableConn.getAdmin();
        if (!admin.isTableAvailable(hbaseTable_cms_volt_curve)) {
            admin.createTable(TableDescriptorBuilder.newBuilder(hbaseTable_cms_volt_curve)//指定表名
                    .setColumnFamilies(Lists.newArrayList(
                            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"))
                                    //指定最多存储多少个历史版本数据
                                    .setMaxVersions(3)
                                    .build()
                    ))
                    .build());
            log.info("hbase表：{}创建成功", cms_volt_curve);
        } else {
            log.info("hbase表：{}已存在", cms_volt_curve);
        }
        createTableConn.close();

        IntStream.range(0, 3).parallel().forEach(i -> {
            String consumerName = "consumer-" + i + "-" + Thread.currentThread().getName();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop)) {
                consumer.subscribe(Collections.singleton(cms_volt_curve));
                List<Put> puts = new ArrayList<>();
                log.info(consumerName + "开始循环拉取数据消费");
                while (true) {
                    log.info(consumerName + "正在拉取数据");
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(interval));
                    log.info(consumerName + "此次拉取消息条数：{}", records.count());
                    if (!records.isEmpty()) {
                        try (Connection conn = HBaseUtil.getHBaseConn(PropertiesUtil.createPropertiesFromResource(ResourcePath.hbase_properties))) {
                            HTable table = (HTable) conn.getTable(hbaseTable_cms_volt_curve);
                            List<Get> gets = new ArrayList<>();
                            Map<String, JsonObject> current_rowKey_JsonObject = new HashMap<>();
                            Map<String, String> history_rowKey_COL_TIME_U = new HashMap<>();
                            records.forEach(cr ->
                                    {
                                        log.info(consumerName + "消费消息 " +
                                                 "topic:" + cr.topic() + "\t" +
                                                 "partition:" + cr.partition() + "\t" +
                                                 "offset:" + cr.offset() + "\t" +
                                                 "value:" + cr.value());
                                        JsonObject jo = JSONUtil.toJsonObject(cr.value());
                                        String rowKey = jo.get("METER_ID").getAsString() + "-" + jo.get("DATA_DATE").getAsString() + "-" + jo.get("PHASE_FLAG").getAsString();
                                        current_rowKey_JsonObject.put(rowKey, jo);
                                        //查询历史
                                        Get get = new Get(Bytes.toBytes(rowKey));
                                        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("COL_TIME-U"));
                                        gets.add(get);
                                    }
                            );
                            //查询此次数据是否有历史数据
                            Result[] results = table.get(gets);

                            if (results != null && results.length != 0) {
                                Arrays.stream(results).parallel().forEach(result -> history_rowKey_COL_TIME_U.put(new String(result.getRow()), new String(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("COL_TIME-U")))));
                            }

                            current_rowKey_JsonObject.forEach((rowKey, jo) -> {
                                String COl_TIME_U = jo.get("COL_TIME").getAsString() + ":" + jo.get("U").getAsString();
                                Put put = new Put(Bytes.toBytes(rowKey));
                                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("METER_ID"), Bytes.toBytes(jo.get("METER_ID").getAsString()));
                                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DATA_DATE"), Bytes.toBytes(jo.get("DATA_DATE").getAsString()));
                                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("PHASE_FLAG"), Bytes.toBytes(jo.get("PHASE_FLAG").getAsString()));
                                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ORG_NO"), Bytes.toBytes(jo.get("ORG_NO").getAsString()));
                                if (history_rowKey_COL_TIME_U.containsKey(rowKey)) {
                                    COl_TIME_U = history_rowKey_COL_TIME_U.get(rowKey) + "," + COl_TIME_U;
                                }
                                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("COL_TIME-U"), Bytes.toBytes(COl_TIME_U));
                                puts.add(put);
                            });
                            log.info(consumerName + "写入hbase puts.size：{} put.example：{}", puts.size(), puts.get(0).toString());
                            table.put(puts);
                            puts.clear();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        log.info(consumerName + " {}s 未拉取到数据，关闭", interval);
                        break;
                    }
                }
            }
        });
        log.info("consumers全部关闭");
        log.info("耗时：{}s", (System.currentTimeMillis() - start) / 1000);
    }

}
