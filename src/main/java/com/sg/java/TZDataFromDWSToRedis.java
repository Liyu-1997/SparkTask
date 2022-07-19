package com.sg.java;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.*;

/**
 * 数据中台台账数据入redis做电压数据计算时候要用的关联关系
 */
public class TZDataFromDWSToRedis {

    private static final Logger log = LoggerFactory.getLogger(TZDataFromDWSToRedis.class);

    //台账数据表
    public static final List<String> tzTable = Lists.newArrayList(
            "un_cms.un14_02_cms_c_meter",
            "dwd_cms.un14_02_cms_g_tg",
            "dwd_cms.un14_02_cms_g_tran",
            "un_cms.un14_02_cms_c_mp_cqzj",
            "un_cms.un14_02_cms_c_meter_mp_rela_cqzj",
            "un_cms.un14_02_cms_c_cons_cqzj"
    );

    public static void main(String[] args) throws Exception {
        log.info("从数据中台dws读取台账数据存入redis");
        log.info("台账数据库表信息：");
        tzTable.forEach(log::info);
        Properties dwsDataProp = PropertiesUtil.createAndLoadPropertiesFromFileOrResource(null, ResourcePath.dws_data_jdbc_properties);
        log.info("数据中台dws配置：" + dwsDataProp.toString());
        Properties redisProp = PropertiesUtil.createAndLoadPropertiesFromFileOrResource(null, ResourcePath.redis_properties);
        log.info("redis配置：" + redisProp.toString());
        try (
                Jedis jedis = new Jedis(redisProp.getProperty("host"), Integer.parseInt(redisProp.getProperty("port")), 1000 * 60);
                Connection conn = DriverManager.getConnection(
                        dwsDataProp.getProperty("url"),
                        dwsDataProp.getProperty("user"),
                        dwsDataProp.getProperty("password")
                )
        ) {
            jedis.auth(redisProp.getProperty("password"));
            jedis.select(0);
            PreparedStatement ps;
            ResultSetMetaData metaData;
            int columnCount;
            int batchSize = 100000;
            Map<String, Object> objMap = new HashMap<>();
            List<String> objMapJsons = new ArrayList<>(batchSize);
            for (String table : tzTable) {
                log.info("当前表：{}", table);
                int index = 0;
                boolean hasMore = true;
                while (hasMore) {
                    ps = conn.prepareStatement("select * from " + table + " limit " + batchSize + " offset " + index + ";");
                    log.info(ps.toString());
                    ResultSet rs = ps.executeQuery();
                    metaData    = rs.getMetaData();
                    columnCount = metaData.getColumnCount();
                    jedis.del(table);
                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            objMap.put(metaData.getColumnLabel(i), rs.getObject(i));
                        }
                        objMapJsons.add(JSONUtil.toJson(objMap));
                        objMap.clear();
                    }
                    objMapJsons.forEach(log::info);
                    jedis.rpush(table, objMapJsons.toArray(new String[0]));
                    index += objMapJsons.size();
                    hasMore = objMapJsons.size() == batchSize;
                }
                objMapJsons.clear();
                log.info("当前表数据同步结束");
            }
            log.info("全部同步结束");
        }
    }

}
