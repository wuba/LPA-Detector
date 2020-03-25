package com.bj58.fbu.graph.algotithm.feature;


import com.bj58.fbu.graph.algotithm.utils.BuildGraphUtils;
import com.bj58.fbu.graph.algotithm.utils.DefaultAttruite;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import scala.Tuple2;

import java.util.Map;

/**
 * @author huangjia03
 */
public class GraphFeature {

    /**
     * 计算图中的每个顶点的度
     *
     * @param g 图
     * @return 顶点包含度的图
     */
    public static VertexRDD<Object> computeDegree(Graph<Map<String, String>, Map<String, String>> g) {
        if (g == null) {
            throw new NullPointerException("图为NULL");
        }
        //计算顶点
        return g.ops().degrees();
    }

    /**
     * 在图属性中添加度属性
     *
     * @param g 图
     * @return 包含度信息的图
     */
    public static Graph<Map<String, String>, Map<String, String>> addProperties(Graph<Map<String, String>, Map<String, String>> g) {
        if (g == null) {
            throw new NullPointerException("输入的数据为NULL");
        }
        VertexRDD<Object> degrees = computeDegree(g);
        JavaPairRDD<Object, Object> map = degrees.toJavaRDD().mapToPair(
                (PairFunction<Tuple2<Object, Object>, Object, Object>) ele ->
                        new Tuple2<>(ele._1, ele._2));
        JavaPairRDD<Object, Map<String, String>> vertexPair = g.vertices()
                .toJavaRDD().mapToPair((PairFunction<Tuple2<Object, Map<String, String>>, Object, Map<String, String>>) ele
                        -> new Tuple2<>(ele._1, ele._2));
        //添加顶点的度到顶点的属性中
        JavaRDD<Tuple2<Object, Map<String, String>>> vertices = vertexPair.join(map)
                .map((Function<Tuple2<Object, Tuple2<Map<String, String>, Object>>,
                        Tuple2<Object, Map<String, String>>>) ele -> {
                    ele._2._1.put(DefaultAttruite.VERTEX_DEGREE, ele._2._2.toString());
                    return new Tuple2<>(ele._1, ele._2._1);
                });
        return BuildGraphUtils.buildGraph(g.edges().toJavaRDD(), vertices);
    }

}
