package com.sg.scala

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

object SparkUtil {

  val log: Logger = LoggerFactory.getLogger(SparkUtil.getClass)

  /**
   * 从kafka创建消息输入流
   *
   * @param ssc    StreamingContext spark-streaming会话
   * @param topics 订阅消息的主题
   * @param prop   连接kafka配置
   * @return 消息输入流
   */
  def createMsgInputStreamFromKafka(ssc: StreamingContext,
                                    topics: String,
                                    prop: Properties)
  : InputDStream[ConsumerRecord[String, String]]
  = {
    //配置topic列表
    val topicSet = topics.split(",").toSet
    log.info(s"订阅主题：$topicSet")
    //配置连接kafka服务端参数
    log.info("配置kafka连接参数")
    val kafkaParams = PropertiesUtil.propToMap(prop)
    //从kafka读取数据的存储策略
    log.info("配置存储策略")
    val locationStrategy = LocationStrategies.PreferConsistent
    //kafka消费策略，订阅主题
    log.info("配置消费策略")
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    //从Kafka接收数据并生成相应的DStream
    log.info(s"正在创建kafka消息数据流 topics:$topics params:$kafkaParams")
    KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategy)
  }

}
