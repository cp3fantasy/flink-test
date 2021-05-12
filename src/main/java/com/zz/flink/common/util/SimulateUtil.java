package com.zz.flink.common.util;

public class SimulateUtil {

    public static String[] createStringArray(String prefix, int count) {
        String[] array = new String[count];
        for (int i = 0; i < count; i++) {
            array[i] = prefix + i;
        }
        return array;
    }

}
