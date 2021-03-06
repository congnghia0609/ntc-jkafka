/*
 * Copyright 2020 nghiatc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ntc.kafka.consumer;

import com.ntc.kafka.util.KConfig;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Jun 7, 2020
 */
/**
 * 
 * KConsumeLoop is swapper of KafkaConsumer
 * @param <K> Key Type [byte[], String, Integer, Long, Float, Double]
 * @param <V> Value Type [byte[], String, Integer, Long, Float, Double]
 */
public abstract class KConsumeLoop<K, V> implements Runnable {
    private final Logger log = LoggerFactory.getLogger(KConsumeLoop.class);

    private final String id;
    private final String name;
    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;
    private final long pollTimeoutMs;
    private final CountDownLatch shutdownLatch;

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public KafkaConsumer<K, V> getConsumer() {
        return consumer;
    }

    public List<String> getTopics() {
        return topics;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }
    
    public KConsumeLoop(Properties props, List<String> topics) {
        this.id = props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG, "KConsumeLoop_" + UUID.randomUUID().toString());
        this.name = id;
        if (topics == null || topics.isEmpty()) {
            throw new ExceptionInInitializerError("topics is NULL or empty.");
        }
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(props);
        this.pollTimeoutMs = Long.valueOf(props.getProperty(KConfig.COMSUMER_POLL_TIMEOUT_MS, "500"));
        this.shutdownLatch = new CountDownLatch(1);
    }
    
    public KConsumeLoop(String name) {
        Properties props = KConfig.getConsumeConfig(name);
        this.id = props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG, name + "_consumer_" + UUID.randomUUID().toString());
        this.name = name;
        this.topics = KConfig.getConsumeTopics(name, null);
        if (topics == null || topics.isEmpty()) {
            throw new ExceptionInInitializerError("Not found config topics.");
        }
        this.consumer = new KafkaConsumer<>(props);
        this.pollTimeoutMs = Long.valueOf(props.getProperty(KConfig.COMSUMER_POLL_TIMEOUT_MS, "500"));
        this.shutdownLatch = new CountDownLatch(1);
    }

    public abstract void process(ConsumerRecord<K, V> record);

    @Override
    public void run() {
        try {
            System.out.println("+++++++ KConsumeLoop[" + id + "] is running on topics: " + topics + ", pollTimeoutMs=" + pollTimeoutMs + "ms");
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                records.forEach(record -> process(record));
            }
        } catch (WakeupException e) {
            // ignore, we're closing
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally {
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        consumer.wakeup();
        shutdownLatch.await();
    }
}
