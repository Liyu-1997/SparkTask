package com.sg.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HBaseUtil {

    private static final Logger log                        = LoggerFactory.getLogger(HBaseUtil.class);

    /**
     * 获取一个hbase连接
     *
     * @param prop 配置文件，可传入代码里编写的自定义配置文件或传null使用默认配置文件：hbase.properties
     * @return
     * @throws IOException
     */
    public static Connection getHBaseConn(Properties prop) {
        Configuration hbaseConf = HBaseConfiguration.create();
        //zookeeper端口号
        log.info("设置zk端口号");
        String port = prop.getProperty("zookeeper.client.port");
        hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        String quorum = prop.getProperty("zookeeper.quorum");
        //zookeeper集群实例地址，和kafka brokers一样，即实例的ip集合，多个逗号隔开
        log.info("设置zk地址");
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, quorum);
        //加载hbase的配置
        log.info("加载hbase配置文件:hbase-site.xml");
        //注意事项：
        //ClassLoader.getSystemResourceAsStream 这个方法获取resource下的资源文件，在用java -jar启动时是可以读取到的，
        //但是用hadoop jar 启动是读取不到的，空指针。华为云客服说是hadoop启动会把InputStream替换成DFSinputstream
        InputStream is = PropertiesUtil.createInputStreamFromFileOrResource(prop.getProperty("hbase-site.path"));
        if (is == null) {
            log.info("无法读取hbase-site.xml，即将退出（这个情况在用hadoop jar启动jar包的情况下会出现，华为云客服工程师说是hadoop环境有个DFSInputStream会替换java.io.InputStream，暂不清楚原因）");
            System.exit(-1);
        }
        hbaseConf.addResource(is);
        log.info("加载hbase配置文件:core-site.xml");
        hbaseConf.addResource(PropertiesUtil.createInputStreamFromFileOrResource(prop.getProperty("core-site.path")));
        log.info("加载hbase配置文件:hdfs-site.xml");
        hbaseConf.addResource(PropertiesUtil.createInputStreamFromFileOrResource(prop.getProperty("hdfs-site.path")));

        log.info("正在获取hbase连接 zk-quorum:" + quorum + " zk-client-port:" + port);
        //获取hbase连接
        try {
            return ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setBasicHBaseConf(Configuration hbaseConf) {
         Properties prop = PropertiesUtil.createPropertiesFromResource(ResourcePath.hbase_properties);
        //zookeeper端口号
        log.info("设置zk端口号");
        String port = prop.getProperty("zookeeper.client.port");
        hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        String quorum = prop.getProperty("zookeeper.quorum");
        //zookeeper集群实例地址，和kafka brokers一样，即实例的ip集合，多个逗号隔开
        log.info("设置zk地址");
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, quorum);
        //加载hbase的配置
        log.info("加载hbase配置文件:hbase-site.xml");
        hbaseConf.addResource(PropertiesUtil.createInputStreamFromFileOrResource(prop.getProperty("hbase-site.path")));
        log.info("加载hbase配置文件:core-site.xml");
        hbaseConf.addResource(PropertiesUtil.createInputStreamFromFileOrResource(prop.getProperty("core-site.path")));
        log.info("加载hbase配置文件:hdfs-site.xml");
        hbaseConf.addResource(PropertiesUtil.createInputStreamFromFileOrResource(prop.getProperty("hdfs-site.path")));
    }

}
