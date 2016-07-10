/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.kafka.stream;

/**
 *
 * @author jeus
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TestTimestampExtractor implements TimestampExtractor {

    private final long base = SmokeTestUtil.START_TIME;

    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
        switch (record.topic()) {
            case "data":
                return base + (Integer) record.value();
            default:
                return System.currentTimeMillis();
        }
    }

}