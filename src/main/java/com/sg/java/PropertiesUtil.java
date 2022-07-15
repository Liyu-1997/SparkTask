package com.sg.java;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Properties createAndLoadPropertiesFromFileOrResource(String absolutePath, String resourcePath) {
        Properties prop = new Properties();
        //优先加载外部配置文件
        if (absolutePath != null) {
            try (FileInputStream fis = new FileInputStream(absolutePath)) {
                log.info("正在加载外部配置文件：{}", absolutePath);
                prop.load(fis);
            } catch (IOException e) {
                //文件未找到再加载内部资源配置
                if (e instanceof FileNotFoundException) {
                    log.info("外部配置文件未找到：{}，正在加载内部配置文件：{}", absolutePath, resourcePath);
                    prop = createPropertiesFromResource(resourcePath);
                } else throw new RuntimeException(e);
            }
        } else {
            //没有外部配置文件加载内部配置文件
            log.info("正在加载内部配置文件：{}", resourcePath);
            prop = createPropertiesFromResource(resourcePath);
        }
        return prop;
    }

    public static Properties createPropertiesFromResource(String resourcePath) {
        Properties prop = new Properties();
        try (InputStream is = ClassLoader.getSystemResourceAsStream(resourcePath)) {
            if (is != null) {
                prop.load(is);
                is.close();
            } else {
                throw new NullPointerException("内部配置文件未找到：" + resourcePath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return prop;
    }

    /**
     * 把文件转成输入流，优先加载resource下的文件，没有再去本地文件系统找（暂未实现hdfs）
     *
     * @param path 文件路径
     * @return 输入流
     */
    public static InputStream createInputStreamFromResourceOrFile(String path) {
        InputStream is;
        is = ClassLoader.getSystemResourceAsStream(path);
        if (is == null) {
            try {
                is = new FileInputStream(path);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return is;
    }

}
