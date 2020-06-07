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
//import com.ntc.configer.NConfig;
//import java.util.Properties;
//import kafka.consumer.Consumer;
//import kafka.consumer.ConsumerConfig;
//import kafka.javaapi.consumer.ConsumerConnector;
//
//
///**
// *
// * @author nghiatc
// * @since Sep 16, 2015
// */
//public abstract class ConsumerConnect {
//	private final ConsumerConnector consumer;
//
//	public ConsumerConnect() {
//		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
//	}
//
//	public ConsumerConnect(String group) {
//		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(group));
//	}
//
//	private ConsumerConfig createConsumerConfig() {
//		Properties props = new Properties();
//		props.put("zookeeper.connect", NConfig.getConfig().getProperty("kafka.consumer.hosts"));
//		props.put("zookeeper.session.timeout.ms", NConfig.getConfig().getProperty("kafka.consumer.timeout"));
//		props.put("zookeeper.sync.time.ms", NConfig.getConfig().getProperty("kafka.consumer.synctime"));
//		props.put("auto.commit.interval.ms", NConfig.getConfig().getProperty("kafka.consumer.autocommit"));
//		props.put("group.id", NConfig.getConfig().getProperty("kafka.consumer.group"));
//		//props.put("consumer.id", NConfig.getConfig().getProperty("kafka.consumer.id"));
//		props.put("rebalance.max.retries", NConfig.getConfig().getProperty("kafka.consumer.retries"));
//
//		return new ConsumerConfig(props);
//
//	}
//
//	private ConsumerConfig createConsumerConfig(String consumerGroup) {
//		Properties props = new Properties();
//		props.put("zookeeper.connect", NConfig.getConfig().getProperty("kafka.consumer.hosts"));
//		props.put("zookeeper.session.timeout.ms", NConfig.getConfig().getProperty("kafka.consumer.timeout"));
//		props.put("zookeeper.sync.time.ms", NConfig.getConfig().getProperty("kafka.consumer.synctime"));
//		props.put("auto.commit.interval.ms", NConfig.getConfig().getProperty("kafka.consumer.autocommit"));
//		props.put("group.id", consumerGroup);
//		//props.put("consumer.id", NConfig.getConfig().getProperty("kafka.consumer.id"));
//		props.put("rebalance.max.retries", NConfig.getConfig().getProperty("kafka.consumer.retries"));
//
//		return new ConsumerConfig(props);
//
//	}
//
//	public ConsumerConnector getConsumer() {
//		return consumer;
//	}
//
//	public abstract int start();
//}
