package com.bj58.fbu.graph.algotithm.predict;


import lombok.Getter;
import lombok.Setter;


/**
 * @author huangjia03
 * @param <O>
 */
public abstract class BaseGraphOps<O> {

    @Getter
    @Setter
    protected String modelName;


    public BaseGraphOps() {
        this.modelName = this.getClass().getSimpleName();
    }

    /**
     * 算法执行
     *
     * @return 输出结果
     */
    public abstract O run();
}
