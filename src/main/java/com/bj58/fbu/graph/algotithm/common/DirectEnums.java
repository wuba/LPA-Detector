package com.bj58.fbu.graph.algotithm.common;


import lombok.Getter;
import org.apache.spark.graphx.EdgeDirection;

/**
 * @author huangjia03
 */
@Getter
public enum DirectEnums {
    /**
     * 迭代方向枚举
     */
    In(1, "in", EdgeDirection.In(), "入射点"),
    Out(2, "out", EdgeDirection.Out(), "出射点"),
    Either(3, "either", EdgeDirection.Either(), "任意收到消息的一边或者所有"),
    Both(4, "Both", EdgeDirection.Both(), "所有");

    private int id;

    private String code;

    private EdgeDirection edgeDirection;

    private String desc;

    DirectEnums(int id, String code, EdgeDirection edgeDirection, String desc) {
        this.id = id;
        this.code = code;
        this.edgeDirection = edgeDirection;
        this.desc = desc;
    }
}
