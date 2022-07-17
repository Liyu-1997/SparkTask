package com.sg.scala

import com.sg.java.{HBaseUtil, ResourcePath}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.UUID

object WriteToHBaseTestDemo {

  val log: Logger = LoggerFactory.getLogger(WriteToHBaseTestDemo.getClass)


  /**
   * 测试MRS集群下HBase的连接和写入
   *
   * @param args 启动配置文件
   */
  def main(args: Array[String]): Unit = {

    log.info("使用参数：<hbase.properties文件路径(可为空，为空默认加载内部配置文件)>")

    log.info("加载hbase配置")
    val prop = com.sg.java.PropertiesUtil.createAndLoadPropertiesFromFileOrResource(
      if (args != null && !args.isEmpty) args(0) else null,
      ResourcePath.hbase_properties
    )
    log.info("创建hbase连接")
    val conn = HBaseUtil.getHBaseConn(prop)

    log.info("生成插入数据value")
    val iterator = Iterator.range(prop.getProperty("number.start.index", "0").toInt, prop.getProperty("number.end.index", "10").toInt)

    val tableName = prop.getProperty("table.name")
    log.info("配置表名：{}", tableName)
    log.info("获取hbase表：{}", tableName)
    val table: HTable = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
    val fqArr = prop.getProperty("family.qualifier")
    log.info("配置列族列名：{}", fqArr)

    log.info("解析列族列名为二元组")
    val fq = fqArr.split(",").map(fq => {
      val familyAndQualifier = fq.split(":")
      (familyAndQualifier(0), familyAndQualifier(1))
    })
    log.info("列族列名：{}", fq.mkString("Array(", ", ", ")"))

    log.info("封装插入数据，row-key为UUID，column-value=数值")
    val puts: Iterator[Put] = iterator.map(value => {
      //构造参数：rowKey
      val put = new Put(Bytes.toBytes(s"${UUID.randomUUID().toString}"))
      //添加列数据，指定列族，具体列，值,多个列有值就addColumn多次
      for (i <- fq) {
        put.addColumn(Bytes.toBytes(i._1), Bytes.toBytes(i._2), Bytes.toBytes(s"$value"))
      }
      put
    })
    val list = new util.ArrayList[Put]()
    puts.foreach(list.add)
    log.info("往hbase里插入数据")
    table.put(list)
    log.info("如需要查看执行结果，登录MRS master节点服务器，上面有hbase的客户端（可能没有，没有的话照着华为云文档下载下客户端），进入到hbase的bin目录启动交互shell，我这边的MRS里hbase在这个目录：opt/client/HBase/hbase/bin，然后当前目录有个hbase脚本，在当前目录输入：hbase shell 即可与hbase库交互，常用命令：list(查看所有表),scan 'tableName'（查询该表数据）")
    log.info("示例操作：")
    log.info(
      s"""
         |cd /opt/client/HBase/hbase/bin
         |hbase shell
         |list
         |scan '$tableName'
         |""".stripMargin)
    log.info("执行完毕，关闭hbase表和连接")
    table.close()
    conn.close()
  }

}
