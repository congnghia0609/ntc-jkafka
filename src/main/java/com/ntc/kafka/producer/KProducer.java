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

package com.ntc.kafka.producer;

import com.ntc.kafka.util.KConfig;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;


/**
 *
 * @author nghiatc
 * @since Sep 16, 2015
 */
/**
 * 
 * KProducer is swapper of Kafka Producer
 * @param <K> Key Type [byte[], String, Integer, Long, Float, Double]
 * @param <V> Value Type [byte[], String, Integer, Long, Float, Double]
 */
public class KProducer<K, V> {

    private static Map<String, KProducer> mapInstanceKProducer = new ConcurrentHashMap<String, KProducer>();
    private static Lock lockInstance = new ReentrantLock();
    private String id;
    private String name;
	private Producer<K, V> producer;

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Producer<K, V> getProducer() {
        return producer;
    }
    
	private KProducer() {
	}
    
    public KProducer(Properties props) {
        id = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG, "KProducer_" + UUID.randomUUID().toString());
        name = id;
		producer = new KafkaProducer<>(props);
	}
    
    public KProducer(String name) {
        Properties props = KConfig.getProduceConfig(name);
        id = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG, name + "_producer_" + UUID.randomUUID().toString());
        this.name = name;
		producer = new KafkaProducer<>(props);
	}
    
    public static KProducer getInstance(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        KProducer instance = mapInstanceKProducer.containsKey(name) ? mapInstanceKProducer.get(name) : null;
        if (instance == null) {
            lockInstance.lock();
            try {
                instance = mapInstanceKProducer.containsKey(name) ? mapInstanceKProducer.get(name) : null;
                if (instance == null) {
                    instance = new KProducer(name);
                    mapInstanceKProducer.put(name, instance);
                }
            } finally {
                lockInstance.unlock();
            }
        }
        return instance;
    }

}
