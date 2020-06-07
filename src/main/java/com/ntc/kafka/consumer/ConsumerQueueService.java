///*
// * Copyright 2015 nghiatc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.ntc.kafka.consumer;
//
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import kafka.consumer.KafkaStream;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
///**
// *
// * @author nghiatc
// * @since Sep 16, 2015
// */
//public class ConsumerQueueService extends ConsumerConnect {
//	private final Logger _logger = LoggerFactory.getLogger(getClass());
//	private List<ConsumerService> consumers = new LinkedList<ConsumerService>();
//
//	public ConsumerQueueService() {}
//
//	@Override
//	public int start() {
//		try {
//			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//			for(ConsumerService consumer : consumers) {
//				topicCountMap.put(consumer.getTopic(), consumer.getNumThread());
//			}
//
//		    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = getConsumer().createMessageStreams(topicCountMap);
//
//		    for(ConsumerService consumer : consumers) {
//		    	List<KafkaStream<byte[], byte[]>> streams =  consumerMap.get(consumer.getTopic());
//			    consumer.assignAndRunStream(streams);
//			    System.out.println("Add stream " + consumer.getTopic() + " success!");
//			}
//
//		} catch (Exception e) {
//			_logger.error("Worker start error ", e);
//			System.out.println("Worker start error !!!");
//		}
//
//	    System.out.println("Worker started !!!");
//		return 0;
//	}
//
//	public void add(ConsumerService service) {
//		consumers.add(service);
//	}
//
//}
