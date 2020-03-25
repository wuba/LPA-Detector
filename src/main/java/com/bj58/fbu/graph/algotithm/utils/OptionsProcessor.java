package com.bj58.fbu.graph.algotithm.utils;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author huangjia
 * Describe: 命令行参数解析
 */
public class OptionsProcessor {

    private static Logger logger = LoggerFactory.getLogger(OptionsProcessor.class);


    /**
     * 入参解析
     *
     * @param args 入参
     * @return 参数集
     * @throws ParseException 解析失败
     */
    public static Map<String, String> parseArg(String[] args) throws ParseException {
        if (args.length <= 0) {
            logger.error("参数为NULL,请输入参数!!!!");
            throw new ParseException("参数为NULL,请输入参数!!!!");
        }
        Map<String, String> options = new HashMap<>();
        Pattern pattern = Pattern.compile("-(\\S+?)=(.+)");
        for (String arg : args) {
            Matcher m = pattern.matcher(arg.trim());
            if (m.find()) {
                String key = m.group(1);
                String value = m.group(2);
                options.put(key, value);
            }
        }
        return options;
    }
}
