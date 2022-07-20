package com.sg.java;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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

import java.util.Iterator;

public class RelCal {

    private static final Logger log = LoggerFactory.getLogger(RelCal.class);

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RelCal");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());


        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DATA_DATE"));
        SingleColumnValueFilter filter_DATA_DATE = new SingleColumnValueFilter(
                Bytes.toBytes("info"), Bytes.toBytes("DATA_DATE"), CompareOperator.EQUAL, Bytes.toBytes("20220716")
        );
        scan.setFilter(filter_DATA_DATE);
        String scanToString = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
        hbConf.set(TableInputFormat.INPUT_TABLE, "cms_volt_curve");
        hbConf.set(TableInputFormat.SCAN, scanToString);

        JavaPairRDD<ImmutableBytesWritable, Result> rdd = jsc.newAPIHadoopRDD(hbConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        rdd.foreachPartition((VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>) iterator -> {

        });


        jsc.stop();
        jsc.close();
    }
}
