package com.sg.scala

import com.sg.java.{KafkaProducerClient, ResourcePath}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Random
import java.util.function.Supplier

/**
 * spark消费kafka客户端
 */
object KafkaSparkConsumerClient {

  val log: Logger = LoggerFactory.getLogger(KafkaSparkConsumerClient.getClass)

  def main(args: Array[String]): Unit = {
    //模拟生产者生产数据，有生产者则可注释
    runProducers()
    //创建spark任务并返回context（context就是一个会话周期）
    val ssc = createSparkTask(args)
    //启动任务
    ssc.start()
    //阻塞一直等待消息进来，不加此代码无法达到实时流式监听处理
    ssc.awaitTermination()
  }

  /**
   * brokers 节点地址，多个逗号隔开 如 192.168.0.1:9092,192.168.0.2:9092
   *
   * @param args
   * @return
   */
  def createSparkTask(args: Array[String]): StreamingContext = {
    val Array(kafkaSparkStreamingPropertiesPath) = args

    log.info(s"spark-streaming接入kafka配置文件路径：$kafkaSparkStreamingPropertiesPath")
    val kafkaSparkStreamingProperties = com.sg.java.PropertiesUtil.createAndLoadPropertiesFromFileOrResource(kafkaSparkStreamingPropertiesPath, ResourcePath.kafka_spark_streaming_hbase_properties)
    log.info(s"spark-streaming接入kafka配置文件内容：$kafkaSparkStreamingProperties")
    val sparkMaster = kafkaSparkStreamingProperties.getProperty("spark.master")
    val batchDuration = kafkaSparkStreamingProperties.getProperty("batch-duration", "5")
    val topics = kafkaSparkStreamingProperties.getProperty("topics")
    val consumerPropertiesPath = kafkaSparkStreamingProperties.getProperty("consumer.properties.path")
    log.info(s"spark master节点地址sparkMaster：$sparkMaster")
    log.info(s"spark流式消费批次间隔batchDuration：$batchDuration")
    log.info(s"订阅的主题topics：$topics")
    log.info(s"kafka消费者配置文件路径consumerPropertiesPath：$consumerPropertiesPath")

    val kafkaConsumerProperties = com.sg.java.PropertiesUtil.createAndLoadPropertiesFromFileOrResource(consumerPropertiesPath, ResourcePath.kafka_consumer_properties)
    log.info(s"kafka消费者配置文件内容：$kafkaConsumerProperties")

    //新建一个Streaming启动环境
    log.info("准备创建sparkConf")
    val sparkConf = new SparkConf().setAppName(KafkaSparkConsumerClient.getClass.getSimpleName)
    //本地启动设置这行代码，提交到MRS spark作业注释，local[*]代表本地启动cpu核数的线程跑任务
    if (sparkMaster != null && sparkMaster == "local[*]") {
      sparkConf.setMaster(sparkMaster)
    }
    //创建spark会话 设置流式批处理的处理间隔 单位s(秒)，即每隔多少秒从kafka拉数据，应该是代理了kafka消费者拉数据的间隔
    log.info("准备创建streamingContext")
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration.toInt))

    //kafka消息输入流
    log.info("准备创建kafka消息输入流")
    val msgInputStream: InputDStream[ConsumerRecord[String, String]] = SparkUtil.createMsgInputStreamFromKafka(ssc, topics, kafkaConsumerProperties)

    //这里做数据计算的
    log.info("准备数据处理算子")
    msgInputStream.foreachRDD(
      rdd => {
        //手动维护偏移量
        //获取偏移量
        //        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //每个RDD分区
        rdd.foreachPartition(
          //消息迭代器
          iterator => {
            //打印每条消息
            iterator.foreach(cr => {
              println(s"消费者客户端消费消息 topic:${cr.topic()}\tpartition:${cr.partition}\toffset:${cr.offset}\tvalue:${cr.value}")
            })
            //            //封装好要存储的数据
            //            val puts: Iterator[Put] = iterator.map(cr => {
            //              //构造参数：rowKey
            //              val put = new Put(Bytes.toBytes(s"spark_put_${UUID.randomUUID().toString}"))
            //              //添加列数据，指定列族，具体列，值,多个列有值就addColumn多次
            //              put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes(s"${cr.value()}"))
            //              put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("address"), Bytes.toBytes(s"${cr.value()}"))
            //            })
            //            //创建一个hbase连接
            //            val conn = HBaseUtil.getHBaseConn(null)
            //            val table: HTable = conn.getTable(TableName.valueOf("first")).asInstanceOf[HTable]
            //            val list = new util.ArrayList[Put]()
            //            puts.foreach(list.add)
            //            log.info("正在写入hbase")
            //            table.put(list)
            //            log.info("关闭连接")
            //            table.close()
            //            conn.close()
          }
        )
        //提交偏移量
        //        msgInputStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    )
    ssc
  }

  /**
   * 创建生产者并开始发送消息
   *
   * @param clientNum 客户端数量
   * @return
   */
  def runProducers(clientNum: Int = 5,
                   topic: String = "mykafka",
                   intervalTime: Long = 1000 * 10,
                   msGenerator: Supplier[String] = new Supplier[String] {

                     val random = new Random()

                     override def get(): String = String.valueOf((random.nextInt(98) + 26).toChar)

                   }): Unit = {
    log.info(s"启动${clientNum}个生产者客户端")
    for (_ <- 0 until clientNum) {
      new KafkaProducerClient(null).start(topic, intervalTime, msGenerator)
    }
  }

}
