/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.kafka.consumer;

/**
 *
 * @author jeus
 */
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerOffsetTest extends ShutdownableThread {

    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public ConsumerOffsetTest(String topic) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.13:9092");//172.17.0.8:2181,172.17.0.9:2181,172.17.0.10:2181");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "que");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        System.out.println("---------------------------------");
        consumer.subscribe(Collections.singletonList(this.topic));
        System.out.println("---------------------------------A");
        while (true) {
            System.out.println("---------------------------------B");
            ConsumerRecords<Integer, String> records = consumer.poll(200000);
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
        }
    }

    
    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}
