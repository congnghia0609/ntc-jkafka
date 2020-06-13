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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Jun 9, 2020
 */
public class WordCountProcessorConsumer {
    private static final Logger log = LoggerFactory.getLogger(WordCountProcessorConsumer.class);
    
    private int numWorker = 1;
    private KConsumerService service = new KConsumerService();
    private final String name = "wordcountprocessor";

    public WordCountProcessorConsumer(int numWorker) {
        this.numWorker = numWorker > 0 ? numWorker : 1;
        for (int i=0; i<this.numWorker; i++) {
            WordCountWorker wcw = new WordCountWorker(name);
            service.addKConsumer(wcw);
        }
    }
    
    public void start() {
        try {
            service.start();
        } catch (Exception e) {
            log.error("WordCountProcessorConsumer start " + e.toString(), e);
        }
    }
    
    public void stop() {
        try {
            service.stop();
        } catch (Exception e) {
            log.error("WordCountProcessorConsumer stop " + e.toString(), e);
        }
    }
    
    public class WordCountWorker extends KConsumeLoop<byte[], byte[]> {

        public WordCountWorker(String name) {
            super(name);
        }

        @Override
        public void process(ConsumerRecord<byte[], byte[]> record) {
            try {
                //System.out.println("====== WordCountWorker[" + getId() + "] is process ======");
                String topic = record.topic();
                String key = new String(record.key());
                String value = new String(record.value());
                System.out.println("topic: " + topic + ", key: " + key + ", value: " + value);
                //System.out.println(record.toString());
                //Thread.sleep(200);
            } catch (Exception e) {
                log.error("WordCountWorker process " + e.toString(), e);
            }
        }
    }
}
