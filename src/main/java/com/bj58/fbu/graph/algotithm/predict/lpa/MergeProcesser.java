package com.bj58.fbu.graph.algotithm.predict.lpa;

import com.bj58.fbu.graph.algotithm.common.AbstractProcessFunc2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huangjia03
 */
public class MergeProcesser extends AbstractProcessFunc2<Map<String, Double>, Map<String, Double>, Map<String, Double>> {

    private Logger logger = LoggerFactory.getLogger(MergeProcesser.class);

    @Override
    public Map<String, Double> apply(Map<String, Double> msg1, Map<String, Double> msg2) {
        if (msg1 == null || msg1.size() <= 0) {
            return msg2;
        }
        if (msg2 == null || msg2.size() <= 0) {
            return msg1;
        }
        Map<String, Double> msg = new HashMap<>(32);
        msg = coalesceMsg(msg1, msg);
        msg = coalesceMsg(msg2, msg);
        msg1 = null;
        msg2 = null;
        return msg;
    }


    /**
     * 修改计算公式
     *
     * @param msg
     * @param result
     * @return
     */
    private Map<String, Double> coalesceMsg(Map<String, Double> msg, Map<String, Double> result) {
        if (result == null) {
            result = new HashMap<>(32);
        }
        if (msg == null || msg.size() <= 0) {
            logger.warn("消息为NULL.....");
            return result;
        }
        for (Map.Entry<String, Double> ms1En : msg.entrySet()) {
            String key = ms1En.getKey();
            if (result.containsKey(key)) {
                result.put(key, ms1En.getValue() + msg.getOrDefault(key, 0.0));
            } else {
                result.put(key, ms1En.getValue());
            }
        }
        return result;
    }

}
