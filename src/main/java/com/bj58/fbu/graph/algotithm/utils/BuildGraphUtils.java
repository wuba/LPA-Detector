package com.bj58.fbu.graph.algotithm.utils;

import lombok.Getter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;

import java.util.Map;
import java.util.Set;

/**
 * @author huangjia03
 * @param <VD>
 * @param <ED>
 */
@Getter
public class BuildGraphUtils<VD, ED> {

    private static Logger logger = LoggerFactory.getLogger(BuildGraphUtils.class);
    /**
     * 顶点
     */
    private JavaRDD<Tuple2<Object, VD>> vertexId;
    /**
     * 边
     */
    private JavaRDD<Edge<ED>> edges;


    /**
     * 默认顶点属性
     */
    private VD defaultVertexAttr;


    /**
     * 根据顶点和边构造图
     *
     * @return 图
     */
    private Graph<VD, ED> constructGraph() {
        if (edges == null) {
            throw new NullPointerException("图构造失败，顶点为NULL or 边为NULL");
        }
        Graph<VD, ED> graph = null;
        if (vertexId != null) {
            try {
                graph = Graph.apply(vertexId.rdd(), edges.rdd(), defaultVertexAttr,
                        StorageLevel.DISK_ONLY(),
                        StorageLevel.DISK_ONLY(),
                        ClassManifestFactory.classType(Object.class),
                        ClassManifestFactory.classType(Object.class));
            } catch (Exception e) {
                throw new NullPointerException("图构建过程失败...");
            }
        } else {
            graph = Graph.fromEdges(edges.rdd(), defaultVertexAttr,
                    StorageLevel.DISK_ONLY(),
                    StorageLevel.DISK_ONLY(),
                    ClassManifestFactory.classType(Object.class),
                    ClassManifestFactory.classType(Object.class));
        }
        return graph;
    }


    private BuildGraphUtils<VD, ED> setVertexId(JavaRDD<Tuple2<Object, VD>> vertexId) {
        this.vertexId = vertexId;
        return this;
    }

    private BuildGraphUtils<VD, ED> setEdges(JavaRDD<Edge<ED>> edges) {
        this.edges = edges;
        return this;
    }

    private BuildGraphUtils<VD, ED> setDefaultVertexAttr(VD defaultVertexAttr) {
        this.defaultVertexAttr = defaultVertexAttr;
        return this;
    }


    /**
     * 构造图
     *
     * @param edge   边
     * @param vertex 顶点
     * @return
     */
    public static Graph<Map<String, String>, Map<String, String>> buildGraph(JavaRDD<Edge<Map<String, String>>> edge,
                                                                             JavaRDD<Tuple2<Object, Map<String, String>>> vertex) {
        if (edge == null) {
            logger.warn("边RDD为NULL");
            throw new NullPointerException("边RDD为NULL");
        }
        if (vertex == null) {
            logger.warn("顶点为NULL");
            throw new NullPointerException("顶点为NULL");
        }
        return new BuildGraphUtils<Map<String, String>, Map<String, String>>()
                .setEdges(edge)
                .setVertexId(vertex)
                .setDefaultVertexAttr(DefaultAttruite.DEFAULT_ATTR)
                .constructGraph();
    }


    /**
     * 构造图
     *
     * @param edge 边
     * @return
     */
    public static Graph<Map<String, String>, Map<String, String>> buildGraph(JavaRDD<Edge<Map<String, String>>> edge) {
        if (edge == null) {
            throw new NullPointerException("边RDD为NULL");
        }
        return new BuildGraphUtils<Map<String, String>, Map<String, String>>()
                .setEdges(edge)
                .setDefaultVertexAttr(DefaultAttruite.DEFAULT_ATTR)
                .constructGraph();
    }


    /**
     * 构造图
     *
     * @param edge   边
     * @param vertex 顶点
     * @return
     */
    public static Graph<long[], Byte> buildGraphByLong(JavaRDD<Edge<Byte>> edge,
                                                       JavaRDD<Tuple2<Object, long[]>> vertex) {
        if (edge == null) {
            logger.warn("边RDD为NULL");
            throw new NullPointerException("边RDD为NULL");
        }
        if (vertex == null) {
            logger.warn("顶点为NULL");
            throw new NullPointerException("顶点为NULL");
        }
        return new BuildGraphUtils<long[], Byte>()
                .setEdges(edge)
                .setVertexId(vertex)
                .setDefaultVertexAttr(DefaultAttruite.defaultAttrLong)
                .constructGraph();
    }


    /**
     * 构造图
     *
     * @param edge   边
     * @param vertex 顶点
     * @return
     */
    public static Graph<long[], Map<String, String>> buildGraphByMapLong(JavaRDD<Edge<Map<String, String>>> edge,
                                                                         JavaRDD<Tuple2<Object, long[]>> vertex) {
        if (edge == null) {
            throw new NullPointerException("边RDD为NULL");
        }
        if (vertex == null) {
            throw new NullPointerException("顶点为NULL");
        }
        return new BuildGraphUtils<long[], Map<String, String>>()
                .setEdges(edge)
                .setVertexId(vertex)
                .setDefaultVertexAttr(DefaultAttruite.defaultAttrLong)
                .constructGraph();
    }


    public static Graph<Set<Long>, Byte> buildGraphBySet(JavaRDD<Edge<Byte>> edge,
                                                         JavaRDD<Tuple2<Object, Set<Long>>> vertex) {
        if (edge == null) {
            throw new NullPointerException("边RDD为NULL");
        }
        if (vertex == null) {
            throw new NullPointerException("顶点为NULL");
        }
        return new BuildGraphUtils<Set<Long>, Byte>()
                .setEdges(edge)
                .setVertexId(vertex)
                .setDefaultVertexAttr(DefaultAttruite.defaultAttrLongSet)
                .constructGraph();
    }
}
