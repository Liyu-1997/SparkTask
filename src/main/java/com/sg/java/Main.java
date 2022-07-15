package com.sg.java;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * 统一入口启动类main方法
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            System.out.println("请指定主类");
            System.exit(-1);
        }
        String className = args[0];
        String[] realArgs = null;
        if (args.length > 1) {
            realArgs = new String[args.length - 1];
            System.arraycopy(args, 1, realArgs, 0, args.length - 1);
        }
        Class<?> mainClass = Class.forName(className);
        System.out.println("已加载类：" + mainClass.getName());
        System.out.println("其他参数：" + Arrays.toString(realArgs));
        Method main = mainClass.getDeclaredMethod("main", String[].class);
        System.out.println("已加载main方法：" + main);
        System.out.println("正在执行");
        main.invoke(null, (Object) realArgs);

    }

}
