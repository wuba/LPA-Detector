package com.bj58.fbu.graph.algotithm.predict.lpa.domain;

import com.google.common.base.Objects;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author huangjia
 * @Date: 2019/9/9 19:38
 * Describe:
 */
public class VertexEnt {
    private Long id;

    private Map<String,String> attr;

    public VertexEnt(Long id, Map<String, String> attr) {
        this.id = id;
        this.attr = attr;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Map<String, String> getAttr() {
        return attr;
    }

    public void setAttr(HashMap<String, String> attr) {
        this.attr = attr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexEnt vertexEnt = (VertexEnt) o;
        return Objects.equal(id, vertexEnt.id) &&
                Objects.equal(attr, vertexEnt.attr);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, attr);
    }
}
