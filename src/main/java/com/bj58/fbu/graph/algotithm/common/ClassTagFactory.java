package com.bj58.fbu.graph.algotithm.common;

import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.util.Map;

/**
 * @author huangjia03
 */
public class ClassTagFactory {

    public static final ClassTag<Map<String, String>> MAP_STR_STR_TAG = ClassManifestFactory.classType(Map.class);

    public static final ClassTag<Map<String, Double>> MAP_STR_DOU_TAG = ClassManifestFactory.classType(Map.class);
}
