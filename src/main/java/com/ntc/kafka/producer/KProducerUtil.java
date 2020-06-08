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

import java.util.ArrayList;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author nghiatc
 * @since Sep 16, 2015
 */
public class KProducerUtil {
	private static Logger logger = LoggerFactory.getLogger(KProducerUtil.class);

	public static Future<RecordMetadata> sendRecordBytes(String name, String topic, String msg) {
		try {
			ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, msg.getBytes("UTF-8"));
			KProducer<byte[], byte[]> kp = KProducer.getInstance(name);
            return kp.getProducer().send(record);
		} catch (Exception e) {
			logger.error("KProducerUtil send ", e);
		}
        return null;
	}

	public static Future<RecordMetadata> sendRecordBytes(String name, String topic, byte[] data) {
		try {
			ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, data);
			KProducer<byte[], byte[]> kp = KProducer.getInstance(name);
            return kp.getProducer().send(record);
		} catch (Exception e) {
			logger.error("KProducerUtil send ", e);
		}
        return null;
	}
}