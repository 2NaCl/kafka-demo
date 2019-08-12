package com.fyj.spark.kafka;

public class KafkaClientApp {
    public static void main(String[] args) {
        new KafkaProducer(KafKaProperties.TOPIC).start();

        new KafkaComsumer(KafKaProperties.TOPIC).start();
    }
}
