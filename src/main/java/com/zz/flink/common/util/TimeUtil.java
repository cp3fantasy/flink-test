package com.zz.flink.common.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimeUtil {

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public static String format(long time){
        return formatter.format(Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()));
    }

    public static void main(String[] args) {
        System.out.println(TimeUtil.format(System.currentTimeMillis()));
    }
}
