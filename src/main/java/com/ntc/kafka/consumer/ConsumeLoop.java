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
public abstract class ConsumeLoop implements Runnable {
    private final Logger log = LoggerFactory.getLogger(ConsumeLoop.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<String> topics;
    private final CountDownLatch shutdownLatch;

    public ConsumeLoop(KafkaConsumer<byte[], byte[]> consumer, List<String> topics) {
        this.consumer = consumer;
        this.topics = topics;
        this.shutdownLatch = new CountDownLatch(1);
    }
    
    // ConsumerConfig ==> Importance.HIGH
    public ConsumeLoop(Properties config, List<String> topics) {
        this.consumer = new KafkaConsumer<>(config);
        this.topics = topics;
        this.shutdownLatch = new CountDownLatch(1);
    }

    public abstract void process(ConsumerRecord<byte[], byte[]> record);

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
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
