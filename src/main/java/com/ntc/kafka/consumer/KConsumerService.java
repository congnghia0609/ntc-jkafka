/*
 * Copyright 2015 nghiatc.
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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Sep 16, 2015
 */
public abstract class KConsumerService {

    private final Logger log = LoggerFactory.getLogger(KConsumerService.class);
    private List<KConsumeLoop> consumers;

    public List<KConsumeLoop> getConsumers() {
        return consumers;
    }

    private KConsumerService() {
    }
    
    public KConsumerService(List<KConsumeLoop> consumers) {
        this.consumers = consumers;
    }

    public void start() {
        try {
            ExecutorService executor = Executors.newFixedThreadPool(consumers.size());
            for (KConsumeLoop kcl : consumers) {
                executor.execute(kcl);
            }
            log.info("KConsumerService start...");
        } catch (Exception e) {
            log.error("KConsumerService start " + e.toString(), e);
        }
    }

}
