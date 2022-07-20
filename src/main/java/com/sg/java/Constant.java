package com.sg.java;

import java.time.LocalDate;

public class Constant {

    public static final String HBASE_NAMESPACE = "cqqkjdyjc";

    public static void main(String[] args) {
        LocalDate now = LocalDate.now();
        LocalDate lastDay = LocalDate.of(now.getYear(), now.getMonth(), now.getDayOfMonth() - 1);
        System.out.println(lastDay.toString().replace('-', Character.MIN_VALUE));
    }

}
