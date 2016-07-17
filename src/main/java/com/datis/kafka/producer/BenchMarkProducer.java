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

public class BenchMarkProducer extends Thread {

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    public static int[] partition = new int[4];
    public static int[] partition1 = new int[4];
    private static final int sleep = 3000;//ms
    private static final int count = 10000;//record
    private static StringBuilder benchLog = new StringBuilder();

    public BenchMarkProducer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.17.0.13:9092");
        props.put("client.id", "OrderingTest");
//      props.put("batch.size",1000);//this for async by size in buffer
//      props.put("linger.ms", 9000);//this for async by milisecond messages buffered
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    private void benchmarkOutput(String type, long startTime, long endTime) {
        for (int i = 0; i < 4; i++) {
            benchLog.append(type + " " + i + ":---->" + partition[i] + "\n");
        }
        benchLog.append("*************************************************\n");
        for (int i = 0; i < 4; i++) {
            benchLog.append(type + " " + i + ":---->" + partition1[i] + "\n");
        }
        benchLog.append((double) (endTime - startTime) / 1000);
        benchLog.append("\n");
        partition = new int[4];
        partition1 = new int[4];
        benchLog.append("-------------------------------------------------\n");
        benchLog.append("-------------------------------------------------\n");
        benchLog.append("-------------------------------------------------\n");
    }
private void sendLog(int key,String value)
{
                System.out.println("MESSAGE SEND->(" + key + ", " + value + ") ");
}
    
    
    @Override
    public void run() {

        try {

            long startTime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                // Send asynchronously
                producer.send(new ProducerRecord(topic, i, "A" + i));
                sendLog(i , "A" + i);
            }
            long endTime = System.currentTimeMillis();
            benchmarkOutput("ASync PARTITION", startTime, endTime);

            Thread.sleep(sleep);
            System.out.println("------------------------------------------------START ASync Callback--------------------------------------------------------");
            startTime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                // Send asynchronously
                DemoCallBack2 back = new DemoCallBack2(startTime, i, "B" + i);
                producer.send(new ProducerRecord(topic, i, "B" + i), back);
                sendLog(i , "B" + i);

            }
            endTime = System.currentTimeMillis();
            benchmarkOutput("ASync Callback PARTITION", startTime, endTime);

            Thread.sleep(sleep);
            System.out.println("------------------------------------------------------START Sync------------------------------------------------------------");
            startTime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                // Send synchronously
                try {
                    RecordMetadata rc = producer.send(new ProducerRecord<>(topic, i, "C" + i)).get();
                   sendLog(i , "C" + i);
                    syncMetadata(i, "C" + i, rc);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            endTime = System.currentTimeMillis();
            benchmarkOutput("Sync PARTITION", startTime, endTime);

            Thread.sleep(sleep);
            System.out.println("-------------------------------------------------START Sync CallBack--------------------------------------------------------");
            startTime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                // Send synchronously
                try {
                    DemoCallBack2 back = new DemoCallBack2(startTime, i, "D" + i);
                    RecordMetadata rc = producer.send(new ProducerRecord<>(topic, i, "D" + i), back).get();
                    sendLog(i , "D" + i);
                    syncMetadata(i, "D" + i, rc);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            endTime = System.currentTimeMillis();
            benchmarkOutput("Sync CallBack PARTITION", startTime, endTime);
        } catch (InterruptedException ex) {
            Logger.getLogger(BenchMarkProducer.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println(benchLog.toString());
    }

    private void syncMetadata(int key, String message, RecordMetadata rc) {
        if (rc != null) {
            BenchMarkProducer.partition[rc.partition()]++;
            System.out.println("**Sync message(" + key + ", " + message + ") sent to partition(" + rc.partition() + "),  offset(" + rc.offset() + ") ,CheckSum(" + rc.checksum() + ")\n");
        } else {
            System.out.println("Exception Is null");
        }
    }

    public static void main(String[] args) {
        BenchMarkProducer pr = new BenchMarkProducer("OrderingTest", true);
        pr.run();

//        Consumer consumer = new Consumer("test");
//        consumer.doWork();
    }

}

class DemoCallBack2 implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack2(long startTime, int key, String message) {
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
//        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            BenchMarkProducer.partition1[metadata.partition()]++;
            System.out.println("##Calb message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), " + "offset(" + metadata.offset() + ") ");
        } else {
            exception.printStackTrace();
        }
    }
}
