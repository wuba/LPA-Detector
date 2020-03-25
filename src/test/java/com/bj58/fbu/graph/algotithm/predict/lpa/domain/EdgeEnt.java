package com.bj58.fbu.graph.algotithm.predict.lpa.domain;

import com.google.common.base.Objects;

import java.util.Map;

/**
 * @Author huangjia
 * @Date: 2019/9/9 19:37
 * Describe:
 */
public class EdgeEnt {

    private Long srcId;

    private Long dstId;

    private Map<String, String> prop;


    public EdgeEnt(Long srcId, Long dstId, Map<String, String> prop) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.prop = prop;
    }

    public Long getSrcId() {
        return srcId;
    }

    public EdgeEnt setSrcId(Long srcId) {
        this.srcId = srcId;
        return this;
    }

    public Long getDstId() {
        return dstId;
    }

    public EdgeEnt setDstId(Long dstId) {
        this.dstId = dstId;
        return this;
    }

    public Map<String, String> getProp() {
        return prop;
    }

    public EdgeEnt setProp(Map<String, String> prop) {
        this.prop = prop;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeEnt edge = (EdgeEnt) o;
        return Objects.equal(srcId, edge.srcId) &&
                Objects.equal(dstId, edge.dstId) &&
                Objects.equal(prop, edge.prop);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(srcId, dstId, prop);
    }
}
