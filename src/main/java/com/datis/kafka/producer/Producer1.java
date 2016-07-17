/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.kafka.producer;

/**
 *
 * @author jeus
 */
import java.lang.reflect.Array;
import java.util.Date;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Producer1 extends Thread {

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    public static int[] partition = new int[4];
    public static int[] partition1 = new int[4];

    public Producer1(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.17.0.13:9092");
        props.put("client.id", "OrderingTest");
//        props.put("batch.size",150);//this for async by size in buffer
//        props.put("linger.ms", 9000);//this for async by milisecond messages buffered
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    @Override
    public void run() {
        for (int i = 0; i < 10000; i++) {

            String messageStr = "M" + i;
            Date newDt = new Date();
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                DemoCallBack1 back = new DemoCallBack1(startTime, i, newDt.toString());
                producer.send(new ProducerRecord(topic, i, newDt.toString()), back);
                System.out.println("BACK-----" + back.toString());
            } else { // Send synchronously
                try {
                    DemoCallBack1 back = new DemoCallBack1(startTime, i, newDt.toString());
                    RecordMetadata rc = producer.send(new ProducerRecord<>(topic, i, newDt.toString()), back).get();
                    syncMetadata(rc);
                    System.out.println("Sent message: (" + i + ", " + messageStr + ")" + rc.partition());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
//            try {
//                Thread.sleep(3000);
//            } catch (InterruptedException ex) {
//                Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
//            }
        }
        System.out.println("PARTION 0 :---->" + partition[0]);
        System.out.println("PARTION 1 :---->" + partition[1]);
        System.out.println("PARTION 2 :---->" + partition[2]);
        System.out.println("PARTION 3 :---->" + partition[3]);
    }

    private void syncMetadata(RecordMetadata rc) {
        if (rc != null) {
            Producer1.partition[rc.partition()]++;
            System.out.println("sent to partition(" + rc.partition() + "), " + "offset(" + rc.offset() + ")");
        } else {
            System.out.println("Exception Is null");
        }
    }

    public static void main(String[] args) {
        Producer1 pr = new Producer1("OrderingTest", true);
        pr.run();

//        Consumer consumer = new Consumer("test");
//        consumer.doWork();
    }

}

class DemoCallBack1 implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack1(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling
     * of request completion. This method will be called when the record sent to
     * the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata The metadata for the record that was sent (i.e. the
     * partition and offset). Null if an error occurred.
     * @param exception The exception thrown during processing of this record.
     * Null if no error occurred.
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            Producer1.partition1[metadata.partition()]++;
            System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
