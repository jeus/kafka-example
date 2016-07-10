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
public class Starter_Consumer {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
//        Producer pr = new Producer("test", Boolean.FALSE);
//        pr.run();
        
        Consumer consumer = new Consumer("test1");
        consumer.doWork();
    }
    
}
