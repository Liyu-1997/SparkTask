package com.sg.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object WordCountDemo {

  val log: Logger = LoggerFactory.getLogger(WordCountDemo.getClass)

  def main(args: Array[String]): Unit = {
    log.info("配置spark")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    log.info("获取spark context")
    val sparkCtx: SparkContext = new SparkContext(sparkConf)
    log.info("读取输入文件")
    val inputFilePath: String = "C:\\Users\\HUAWEI\\Desktop\\word.txt"
    log.info("转RDD")
    val fileRDD: RDD[String] = sparkCtx.textFile(inputFilePath)
    log.info("计算")
    val value: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    log.info("输出计算结果")
    value.foreach(println(_))
    log.info("停止spark context")
    sparkCtx.stop()
  }

}
