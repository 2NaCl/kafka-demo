package com.fyj.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * 消费的时候主要用的是zookeeper
 * 生产主要依靠broker-list
 */
public class KafkaComsumer extends Thread{

    private String topic;

    public KafkaComsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConnector() {

        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafKaProperties.ZK);
        properties.put("group.id", KafKaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumerConnector = createConnector();

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        //  String:topic
        //  List<KafkaStream<byte[], byte[]>>对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> messageAndMetadata = messageStreams.get(topic).get(0);//获取每一次的数据

        ConsumerIterator<byte[], byte[]> iterator = messageAndMetadata.iterator();//使用迭代器遍历得到每次的数据

        while (iterator.hasNext()) {
            String s = new String(iterator.next().message());
            System.out.println("Consumer"+s);
        }
    }
}
