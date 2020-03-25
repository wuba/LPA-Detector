package com.bj58.fbu.graph.algotithm.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * @author huangjia03
 */
public class ComparableUtils {

    /**
     * Map的value值降序序排序
     *
     * @param map 输入
     * @param nn top n
     * @param <K> k类型
     * @param <V> value 类型
     * @return 排序输出
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> soreDesc(Map<K, V> map, int nn) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2) -> {
            int compare = (o1.getValue()).compareTo(o2.getValue());
            return -compare;
        });

        Map<K, V> returnMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            if (nn > 0) {
                returnMap.put(entry.getKey(), entry.getValue());
            }
            nn--;
        }
        return returnMap;
    }
}
