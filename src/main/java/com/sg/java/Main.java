package com.sg.java;

import com.sg.scala.KafkaSparkConsumerClient;
import com.sg.scala.WordCountDemo;
import com.sg.scala.WriteToHBaseTestDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * 统一入口启动类main方法
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            log.info("参数不可为空，请按照该格式：java -jar xx.jar <主类名> ...<该主类main方法运行所需参数>");
            log.info("可用主类方法：");
            log.info(WordCountDemo.class.getName());
            log.info(WriteToHBaseTestDemo.class.getName());
            log.info(KafkaSparkConsumerClient.class.getName());
            System.exit(-1);
        }
        String className = args[0];
        String[] realArgs = null;
        if (args.length > 1) {
            realArgs = new String[args.length - 1];
            System.arraycopy(args, 1, realArgs, 0, args.length - 1);
        }
        Class<?> mainClass = Class.forName(className);
        log.info("已加载类：" + mainClass.getName());
        log.info("其他参数：" + Arrays.toString(realArgs));
        Method main = mainClass.getDeclaredMethod("main", String[].class);
        log.info("已加载main方法：" + main);
        log.info("正在执行");
        main.invoke(null, (Object) realArgs);

    }

}
