package com.bj58.fbu.graph.algotithm.predict.lpa;


import com.bj58.fbu.graph.algotithm.common.AbstractProcessFunc3;
import com.bj58.fbu.graph.algotithm.utils.ComparableUtils;

import java.util.Map;

/**
 * @author huangjia03
 */
public class VertexProcesser extends AbstractProcessFunc3<Object, Map<String, String>, Map<String, Double>, Map<String, String>> {


    private String label;

    public VertexProcesser(String label) {
        this.label = label;
    }

    /**
     * 传播出来的标签key
     */
    public static final String PROPAGATE_LABEL = "propagateLabel";

    @Override
    public Map<String, String> apply(Object vid, Map<String, String> attr, Map<String, Double> msg) {
        if (msg == null || msg.size() <= 0) {
            return attr;
        }
        //如果顶点包含了原始标签-则不参与更新消息
        if (attr.containsKey(label)) {
            return attr;
        }
        //那个消息权值大，就更新成那个
        Map<String, Double> soreDesc = ComparableUtils.soreDesc(msg, 1);
        for (Map.Entry<String, Double> entry : soreDesc.entrySet()) {
            attr.put(PROPAGATE_LABEL, entry.getKey());
        }
        return attr;
    }
}
