package com.bj58.fbu.graph.algotithm.predict.lpa;

import com.bj58.fbu.graph.algotithm.common.AbstractProcessFunc1;
import com.bj58.fbu.graph.algotithm.utils.DefaultAttruite;
import lombok.Setter;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;

import java.util.*;

/**
 * @author huangjia03
 */
@Setter
public class SendProcesser extends AbstractProcessFunc1<EdgeTriplet<Map<String, String>, Map<String, String>>,
        Iterator<Tuple2<Object, Map<String, Double>>>> {

    private Logger logger = LoggerFactory.getLogger(SendProcesser.class);
    /**
     * 默认权值
     */
    private final double DEFAULT_W = 0.1;

    private String label;

    public static final String PROPAGATE_LABEL = "propagateLabel";

    private String type;
    /**
     * 权值
     */
    private Broadcast<Map<String, Double>> weightBc;

    SendProcesser(String label, String type, Broadcast<Map<String, Double>> weightBc) {
        this.label = label;
        this.type = type;
        this.weightBc = weightBc;
    }


    @Override
    public Iterator<Tuple2<Object, Map<String, Double>>> apply(EdgeTriplet<Map<String, String>, Map<String, String>> edge) {
        if (edge == null) {
            logger.error("边的三元组为NULL......");
            return JavaConversions.asScalaIterator(Collections.emptyIterator());
        }
        double dstLabelW = initWeight(edge.dstAttr(), DEFAULT_W);
        double srcLabelW = initWeight(edge.srcAttr(), DEFAULT_W);
        double edgeLabelW = initWeight(edge.attr, DEFAULT_W);
        String dstDegree = edge.dstAttr()
                .getOrDefault(DefaultAttruite.VERTEX_DEGREE, String.valueOf(DEFAULT_W));
        String srcDegree = edge.srcAttr()
                .getOrDefault(DefaultAttruite.VERTEX_DEGREE, String.valueOf(DEFAULT_W));

        if (edge.dstAttr().containsKey(label) && edge.srcAttr().containsKey(label)) {
            return JavaConversions.asScalaIterator(Collections.emptyIterator());
        }
        //没有标签,不进行消息发送
        if (!edge.dstAttr().containsKey(label) && !edge.srcAttr().containsKey(label) &&
                !edge.dstAttr().containsKey(PROPAGATE_LABEL) && !edge.srcAttr().containsKey(PROPAGATE_LABEL)) {
            return JavaConversions.asScalaIterator(Collections.emptyIterator());
        }

        List<Tuple2<Object, Map<String, Double>>> msg = new ArrayList<>();
        Map<String, Double> content = new HashMap<>(32);
        if (edge.dstAttr().containsKey(label)
                && !edge.srcAttr().containsKey(label)) {
            content.put(edge.dstAttr().get(label), calcWeight(dstLabelW, edgeLabelW, Double.parseDouble(dstDegree)));
            msg.add(new Tuple2<>(edge.srcId(), content));
            return JavaConversions.asScalaIterator(msg.iterator());
        }
        if (edge.srcAttr().containsKey(label)
                && !edge.dstAttr().containsKey(label)) {
            content.put(edge.srcAttr().get(label), calcWeight(srcLabelW, edgeLabelW, Double.parseDouble(srcDegree)));
            msg.add(new Tuple2<>(edge.dstId(), content));
            return JavaConversions.asScalaIterator(msg.iterator());
        }
        if (edge.dstAttr().containsKey(PROPAGATE_LABEL) && !edge.srcAttr().containsKey(PROPAGATE_LABEL)) {
            content.put(edge.dstAttr().get(PROPAGATE_LABEL), calcWeight(dstLabelW, edgeLabelW, Double.parseDouble(dstDegree)));
            msg.add(new Tuple2<>(edge.srcId(), content));
            return JavaConversions.asScalaIterator(msg.iterator());
        }
        if (edge.srcAttr().containsKey(PROPAGATE_LABEL) && !edge.dstAttr().containsKey(PROPAGATE_LABEL)) {
            content.put(edge.srcAttr().get(PROPAGATE_LABEL), calcWeight(dstLabelW, edgeLabelW, Double.parseDouble(srcDegree)));
            msg.add(new Tuple2<>(edge.dstId(), content));
            return JavaConversions.asScalaIterator(msg.iterator());
        }
        Map<String, Double> content2 = new HashMap<>(32);
        content.put(edge.srcAttr().get(PROPAGATE_LABEL), calcWeight(srcLabelW, edgeLabelW, Double.parseDouble(srcDegree)));
        content2.put(edge.dstAttr().get(PROPAGATE_LABEL), calcWeight(dstLabelW, edgeLabelW, Double.parseDouble(dstDegree)));
        msg.add(new Tuple2<>(edge.dstId(), content));
        msg.add(new Tuple2<>(edge.srcId(), content2));
        return JavaConversions.asScalaIterator(msg.iterator());
    }


    /**
     * 初始化标签权重
     *
     * @param attr     标签
     * @param defaultW 默认权值
     * @return 点权重
     */
    private double initWeight(Map<String, String> attr, double defaultW) {
        if (attr == null || attr.size() <= 0) {
            logger.error("顶点属性不存在，返回默认值");
            return defaultW;
        }
        /**
         *没有任何权值设置
         */
        if (weightBc.value() == null) {
            return defaultW;
        }
        for (Map.Entry<String, Double> entry : weightBc.value().entrySet()) {
            if (attr.containsKey(type)) {
                if (attr.get(type).equals(entry.getKey())) {
                    return entry.getValue();
                }
            }
            if (attr.containsKey(label)) {
                if (attr.get(label).equals(entry.getKey())) {
                    return entry.getValue();
                }
            }
            if (attr.containsKey(PROPAGATE_LABEL)) {
                if (attr.get(PROPAGATE_LABEL).equals(entry.getKey())) {
                    return entry.getValue();
                }
            }
        }
        return defaultW;
    }


    /**
     * 计算权值
     *
     * @param lableW  标签的权值
     * @param edgeW   关系的权值SendProcesser
     * @param degreeW 度分布的权值
     */
    private double calcWeight(double lableW, double edgeW, double degreeW) {
        if (Objects.equals(lableW, DEFAULT_W) && Objects.equals(lableW, DEFAULT_W) && Objects.equals(lableW, DEFAULT_W)) {
            if (logger.isDebugEnabled()) {
                logger.warn("边-顶点[label]-度 的权值为 label:[{}],edgeW:[{}],degreeW :[{}]", lableW, edgeW, degreeW);
            }
            return DEFAULT_W;
        }
        return Math.exp(edgeW + lableW);
    }
}
