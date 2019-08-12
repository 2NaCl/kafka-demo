package com.fyj.spark.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * kafka生产者，需要topic和broker-list
 */
public class KafkaProducer extends Thread{

    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        //建立握手机制：要求服务器端对客户端的消息进行反馈
        // 0：不等待握手机制反馈
        // 1：服务端将请求写入日志，然后立刻响应握手 为了不丢失数据，大多数都选择1
        // -1：同步等待所有副本的握手机制，最安全
        properties.put("request.required.acks", "1");
        //设置序列化
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        //建立broker-list
        properties.put("metadata.broker.list", KafKaProperties.BROKER_LIST);


        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }
    //用线程的方式开始测试
    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String message = "message_" + messageNo;//获取消息内容
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent:" + message);
            messageNo++;

            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
