package com.zz.flink.common.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtil {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String format(long time){
        return formatter.format(Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()));
    }

    public static long toMillis(String timeStr){
        LocalDateTime ldt = LocalDateTime.from(formatter.parse(timeStr));
        ZonedDateTime zdt = ZonedDateTime.of(ldt, ZoneId.systemDefault());
        return zdt.toInstant().toEpochMilli();
    }

    public static void main(String[] args) {
        long now = System.currentTimeMillis();
        System.out.println(now);
        String timeStr = format(now);
        System.out.println(timeStr);
        long t = toMillis(timeStr);
        System.out.println(t);
    }
}
