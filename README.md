
# 基于Graphx 的 LPA 算法改进
LPA 是一种基于标签传播的局部社团划分算法，而GraphX 是一种优秀的并行的图计算框架。
将两者结合在一起能够大大加快整个算法的迭代效率，并且对算法的迭代我们进行了一定的优化，
能够对不同的标签和不同的关系设定置信权重，更好的优化我们的迭代结果，
帮助我们快速迭代算法和进行风险传导。


## 应用介绍
### 参考文档
LPA 的相关论文参考 [Paper文件]()。

Graphx 的详细说明文档：[GraphX说明文档](http://spark.apache.org/)

### 现状
1. LPA 主要实现是基于Python 或者 R 实现，并且是单机运行，在大规模的数据下，无法进行迭代。
2. Graphx 自带的LPA 算法，无法自定义的label标签，并且无法设置标签和边的置信权重，无法优化迭代效果。

 
## 应用介绍
1. 将已知的用户节点标签通过关系传播到无标签的用户结点上，传播结果以评分的形式使用。例如，在风控场景中通过风险传导的方式，将”好坏”用户的标签传播给周围的邻居（一度 ，二度.....）节点。
2. 可调节的用户结点置信权重和边权值,例如，风控场景中，坏用户对周边节点的影响力大于好用户，即：坏用户的置信权重高于好用户。共同使用手机号或者身份证号的边关系大于共同使用的IP的边关系。
3. 优化标签传播算法的迭代效率。优化了节点间的消息传递机制优，减少不必要网络开销，提高了迭代效率。

## 运行环境
1. Spark 2.3.1 +
2. JDK 8+
3. Graphx 2.3.1+
4. scala 2.11

**默认情况下，在Spark集群运行时满足以上的条件的，并且我们需要构造Graph 用于迭代。**

## 使用方式
### 图的结构
输入的图的RDD
1. edge：Edge\<Map\<String,String\>\>
2. vertices:Tuple2\<Long,Map\<String,Stgring\>\>
 
### 参数说明
LabelPropagationOps 类主要参数如下：
1. graph:输入的图，结构如上。
2. maxInterator : 设定的最大迭代次数。
3. directEnums：消息传播的方向
4. weight：权重Map，如 ：pass=0.3 is_parent=0.9
5. labelKey：需要进行传播标签的key名称 ，如：label=pass，label=refuse。那么这个labelKey = label
6. type：关系权重和顶点权值的类型的key ，如：V(type=user，type=phone .....)，edge(type=is_idCardNo....)

### 运行
```
  LabelPropagationOps ops = new LabelPropagationOps();
  ops.run;
```
### 结果说明
结果是一个图，属性结构和输入的属性结构一致。顶点包含了一个key：propagateLabel ，是传播结果的标签。


## 迭代图形结构说明
1. 如果输入的图示有向图，并且方向对迭代效果影响较大，需要设定directEnums 参数
2. 如果是无向图或者说方向不是特别重要，可以设定EdgeDirection.Either。

## 注意事项
LPA是属于非收敛的算法，我们需要设定最大迭代次数，一般情况是我们考虑一个节点对几度内关系节点有所影响。


## 性能度量
生产环境数据量：1亿+ 的顶点  和 40亿+ 的边基础上。标签样本：3千万+ 

环境配制：200 core ，单机20G 内存

迭代时间如图：


每轮迭代的时间大概在35分钟左右，随着迭代次数的增加，迭代时间有所增加。


以上数据量在Python 或者 R 实现的单机环境下无法进行有效迭代。

## Copyright and License
todo











 



