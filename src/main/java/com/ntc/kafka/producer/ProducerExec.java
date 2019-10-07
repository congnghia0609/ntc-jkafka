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

import com.ntc.configer.NConfig;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 *
 * @author nghiatc
 * @since Sep 16, 2015
 */
public class ProducerExec {

	private Producer<byte[], byte[]> producer;
	public static ProducerExec Instance = new ProducerExec();

	public ProducerExec() {
		init();
	}

	private void init() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, NConfig.getConfig().getProperty("kafka.broker"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, NConfig.getConfig().getProperty("kafka.serializer"));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, NConfig.getConfig().getProperty("kafka.serializer"));
		props.put(ProducerConfig.ACKS_CONFIG, NConfig.getConfig().getString("kafka.ack", "1"));
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, NConfig.getConfig().getString("kafka.buffer", "33554432"));
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, NConfig.getConfig().getString("kafka.compression", "none"));
		props.put(ProducerConfig.RETRIES_CONFIG, NConfig.getConfig().getString("kafka.retries", "0"));
		props.put(ProducerConfig.CLIENT_ID_CONFIG, NConfig.getConfig().getString("kafka.clientId", "socket"));

		producer = new KafkaProducer<byte[], byte[]>(props);
	}

	public void send(ProducerRecord<byte[], byte[]> record) {
		Future<RecordMetadata> result = producer.send(record);
	}

}
