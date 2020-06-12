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

package com.ntc.app;

import com.ntc.kafka.consumer.KConsumeLoop;
import com.ntc.kafka.consumer.KConsumerService;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Jun 8, 2020
 */
public class EmailConsumer {
    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);
    
    private int numWorker = 1;
    private KConsumerService service = new KConsumerService();
    private final String name = "worker";
    private List<String> topics = new ArrayList<>();

    public EmailConsumer(int numWorker) {
        this.numWorker = numWorker > 0 ? numWorker : 1;
        this.topics.add("email");
        for (int i=0; i<this.numWorker; i++) {
            EmailWorker ew = new EmailWorker(name, topics);
            service.addKConsumer(ew);
        }
    }
    
    public void start() {
        try {
            service.start();
        } catch (Exception e) {
            log.error("EmailConsumer start " + e.toString(), e);
        }
    }
    
    public void stop() {
        try {
            service.stop();
        } catch (Exception e) {
            log.error("EmailConsumer stop " + e.toString(), e);
        }
    }
    
    public class EmailWorker extends KConsumeLoop<byte[], byte[]> {

        public EmailWorker(String name, List<String> topics) {
            super(name, topics);
        }

        @Override
        public void process(ConsumerRecord<byte[], byte[]> record) {
            try {
                System.out.println("====== EmailWorker[" + getId() + "] is process ======");
                String topic = record.topic();
                //String key = new String(record.key(), "UTF-8");
                String value = new String(record.value(), "UTF-8");
                System.out.println("topic: " + topic + ", value: " + value);
                System.out.println(record.toString());
                //Thread.sleep(200);
            } catch (Exception e) {
                log.error("EmailWorker process " + e.toString(), e);
            }
        }
    }
}
