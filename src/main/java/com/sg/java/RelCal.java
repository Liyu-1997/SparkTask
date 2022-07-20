package com.sg.java;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Iterator;

public class RelCal {

    private static final Logger log = LoggerFactory.getLogger(RelCal.class);

    public static void main(String[] args) throws Exception {

        log.info("创建sparkConf");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RelCal");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator");

        log.info("创建sparkContext");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        log.info("创建hbaseConf");
        Configuration hbaseConf = HBaseConfiguration.create(jsc.hadoopConfiguration());

        log.info("设置hbaseConf默认配置");
        HBaseUtil.setBasicHBaseConf(hbaseConf);

        log.info("创建hbase扫描器scan");
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DATA_DATE"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");




        String lastDate;

        log.info("扫描器设置过滤器filters");
        scan.setFilter(new FilterList(
                new SingleColumnValueFilter(
                        Bytes.toBytes("info"), Bytes.toBytes("DATA_DATE"), CompareOperator.EQUAL, Bytes.toBytes("20220716")
                ),
                new SingleColumnValueFilter(
                        Bytes.toBytes("info"), Bytes.toBytes("DATA_DATE"), CompareOperator.EQUAL, Bytes.toBytes("20220716")
                )
        ));


        String scanToString = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "cms_volt_curve");
        log.info("scanToString：{}", scanToString);
        hbaseConf.set(TableInputFormat.SCAN, scanToString);

        log.info("从hbase获取rdd");
        JavaPairRDD<ImmutableBytesWritable, Result> rdd = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        log.info("算子逻辑");
        rdd.foreachPartition((VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>) iterator -> {
            while (iterator.hasNext()) {
                Tuple2<ImmutableBytesWritable, Result> item = iterator.next();
                Result result = item._2;

            }
        });


        log.info("处理结束");
        jsc.stop();
        jsc.close();
    }

}
