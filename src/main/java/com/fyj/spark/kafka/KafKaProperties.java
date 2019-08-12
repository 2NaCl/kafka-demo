package com.fyj.spark.kafka;

/**
 * kafka常用配置文件
 * 需要配置Zookeeper，broker-list，topic
 */
public class KafKaProperties {

    public static String ZK = "192.168.243.20:2181";

    public static String TOPIC = "test";

    public static String BROKER_LIST = "192.168.243.20:9092";

    public static final String GROUP_ID = "test_group1";

}
