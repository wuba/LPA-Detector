package com.bj58.fbu.graph.algotithm.predict.lpa;

import com.bj58.fbu.graph.algotithm.common.ClassTagFactory;
import com.bj58.fbu.graph.algotithm.feature.GraphFeature;
import com.bj58.fbu.graph.algotithm.predict.BaseGraphOps;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.Pregel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassManifestFactory;

import java.util.Map;

/**
 * @author huangjia03
 */
@Getter
@Setter
public class LabelPropagationOps extends BaseGraphOps<Graph<Map<String, String>, Map<String, String>>> {

    private Logger logger = LoggerFactory.getLogger(LabelPropagationOps.class);

    /**
     * 最大迭代次数
     */
    private int maxInterator;

    /**
     * 方向
     */
    private EdgeDirection directEnums;


    /**
     * 初始化消息
     */
    private Map<String, Double> initMessage;

    /**
     * 权重
     */
    private Map<String, Double> weight;

    private String labelKey;

    private String type;

    /**
     * 图
     */
    private Graph<Map<String, String>, Map<String, String>> graph;

    private Broadcast<Map<String, Double>> weightBc;

    public LabelPropagationOps() {
    }


    public LabelPropagationOps(int maxInterator, EdgeDirection directEnums, Map<String, Double> initMessage, Map<String, Double> weight,
                               String labelKey, Graph<Map<String, String>, Map<String, String>> graph, Broadcast<Map<String, Double>> weightBc) {
        this.maxInterator = maxInterator;
        this.directEnums = directEnums;
        this.initMessage = initMessage;
        this.weight = weight;
        this.labelKey = labelKey;
        this.graph = graph;
        this.weightBc = weightBc;
    }

    public LabelPropagationOps(int maxInterator, EdgeDirection directEnums, Map<String, Double> weight, String labelKey,
                               Graph<Map<String, String>, Map<String, String>> graph) {
        this.maxInterator = maxInterator;
        this.directEnums = directEnums;
        this.weight = weight;
        this.labelKey = labelKey;
        this.graph = graph;
    }

    /**
     * 图迭代
     *
     * @return 结果图
     */
    @Override
    public Graph<Map<String, String>, Map<String, String>> run() {
        if (graph == null) {
            throw new NullPointerException("输入的图的NULL, 请检查数据是够存在....");
        }
        if (maxInterator <= 0) {
            throw new NullPointerException(String.format("迭代次数必须大于等于0,输入的迭代次数为：[%d]", maxInterator));
        }
        weightBc = graph.vertices().sparkContext().broadcast(weight,
                ClassManifestFactory.classType(Map.class));
        //计算度值
        this.graph = GraphFeature.addProperties(graph);
        return Pregel.apply(this.graph,
                initMessage, maxInterator,
                directEnums,
                new VertexProcesser(this.labelKey),
                new SendProcesser(this.labelKey, this.type, this.weightBc),
                new MergeProcesser(),
                ClassTagFactory.MAP_STR_STR_TAG,
                ClassTagFactory.MAP_STR_STR_TAG,
                ClassTagFactory.MAP_STR_DOU_TAG);
    }
}
