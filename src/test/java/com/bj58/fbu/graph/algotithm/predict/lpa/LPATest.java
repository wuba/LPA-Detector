package com.bj58.fbu.graph.algotithm.predict.lpa;

import com.alibaba.fastjson.JSONObject;
import com.bj58.fbu.graph.algotithm.predict.lpa.domain.EdgeEnt;
import com.bj58.fbu.graph.algotithm.predict.lpa.domain.VertexEnt;
import com.bj58.fbu.graph.algotithm.utils.BuildGraphUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Author huangjia
 * @Date: 2019/12/24 17:48
 * Describe:
 */
public class LPATest implements java.io.Serializable {

    private static final String PROPAGATE_LABEL = "propagateLabel";

    private Graph<Map<String, String>, Map<String, String>> graph;

    private JavaSparkContext jsc;

    @Before
    public void before() {
        SparkConf conf = new SparkConf();
        conf.setAppName("lpaRunning");
        conf.setMaster("local");
        //测试
        conf.set("spark.driver.allowMultipleContexts", "true");
        if (jsc == null) {
            jsc = new JavaSparkContext(conf);
        }
        JavaRDD<Edge<Map<String, String>>> edge = jsc.textFile(Objects.requireNonNull(LPATest.class.getClassLoader()
                .getResource("no-weight/edge.txt")).getPath())
                .map((Function<String, Edge<Map<String, String>>>) ele -> {
                    EdgeEnt edgeEnt = JSONObject.parseObject(ele, EdgeEnt.class);
                    return new Edge<>(edgeEnt.getSrcId(), edgeEnt.getDstId(), edgeEnt.getProp());
                });
        JavaRDD<Tuple2<Object, Map<String, String>>> vertices = jsc.textFile(Objects.requireNonNull(LPATest.class.getClassLoader()
                .getResource("no-weight/vertex.txt")).getPath())
                .map((Function<String, Tuple2<Object, Map<String, String>>>) ele -> {
                    VertexEnt vertexEnt = JSONObject.parseObject(ele, VertexEnt.class);
                    return new Tuple2<>(vertexEnt.getId(), vertexEnt.getAttr());
                });
        //构造图
        graph = BuildGraphUtils.buildGraph(edge, vertices);
    }

    /**
     * 验证图迭代
     */
    @Test
    public void lpaRunning1() {
        LabelPropagationOps ops = new LabelPropagationOps();
        ops.setDirectEnums(EdgeDirection.Either());
        ops.setGraph(graph);
        ops.setGraph(graph);
        //标签
        ops.setLabelKey("label");
        //最大迭代次数
        ops.setMaxInterator(10);
        //类型key
        ops.setType("type");
        Graph<Map<String, String>, Map<String, String>> resultGraph = ops.run();
        resultGraph.vertices().toJavaRDD().take(10);
        jsc.stop();
    }

    /**
     * 验证结果
     */
    @Test
    public void lpaRunning2() {
        LabelPropagationOps ops = new LabelPropagationOps();
        ops.setDirectEnums(EdgeDirection.Either());
        ops.setGraph(graph);
        ops.setGraph(graph);
        //标签
        ops.setLabelKey("label");
        //最大迭代次数
        ops.setMaxInterator(10);
        //类型key
        ops.setType("type");
        Graph<Map<String, String>, Map<String, String>> resultGraph = ops.run();
        List<Tuple2<Object, Map<String, String>>> vertexList = resultGraph.vertices().toJavaRDD().take(10);
        for (Tuple2<Object, Map<String, String>> tuple2 : vertexList) {
            if (tuple2._1.equals(10)) {
                assert tuple2._2.containsKey(PROPAGATE_LABEL) || tuple2._2.get(PROPAGATE_LABEL).equals("red");
            }
        }
        jsc.stop();
    }

    /**
     * 验证结果
     */
    @Test
    public void lpaRunning3() {
        LabelPropagationOps ops = new LabelPropagationOps();
        ops.setDirectEnums(EdgeDirection.Either());
        ops.setGraph(graph);
        ops.setGraph(graph);
        //标签
        ops.setLabelKey("label");
        //最大迭代次数
        ops.setMaxInterator(10);
        //类型key
        ops.setType("type");
        Graph<Map<String, String>, Map<String, String>> resultGraph = ops.run();
        List<Tuple2<Object, Map<String, String>>> vertexList = resultGraph.vertices().toJavaRDD().take(10);
        for (Tuple2<Object, Map<String, String>> tuple2 : vertexList) {
            if (tuple2._1.equals(2)) {
                assert tuple2._2.containsKey(PROPAGATE_LABEL) || tuple2._2.get(PROPAGATE_LABEL).equals("blue");
            }
        }
        jsc.stop();
    }


}
