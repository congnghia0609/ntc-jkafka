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

package com.ntc.kafka.util;

import com.ntc.configer.NConfig;
import java.util.Properties;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Jun 8, 2020
 */
public class KConfig {
    
    private static final Logger log = LoggerFactory.getLogger(KConfig.class);
    public static final String COMSUMER_PREFIX = ".kafka.consumer.";
    public static final String PRODUCER_PREFIX = ".kafka.producer.";
    public static final String STREAM_PREFIX = ".kafka.stream.";
    
    public static final String COMSUMER_POLL_TIMEOUT_MS = "poll.timeout.ms";
    
    public static Properties getConsumeConfig(String name) {
        Properties props = new Properties();
        try {
            Set<String> setName = ConsumerConfig.configNames();
            for (String cname : setName) {
                String nkey = name + COMSUMER_PREFIX + cname;
                Object nvalue = NConfig.getConfig().containsKey(nkey) ? NConfig.getConfig().getProperty(nkey) : null;
                if (nvalue != null) {
                    props.put(cname, nvalue);
                }
            }
            String id = name + "_consumer_" + UUID.randomUUID().toString();
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id);
            // Customize Properties
            String pkey = name + COMSUMER_PREFIX + COMSUMER_POLL_TIMEOUT_MS;
            String pollTimeout = String.valueOf(NConfig.getConfig().getLong(pkey, 500L));
            props.put(COMSUMER_POLL_TIMEOUT_MS, pollTimeout);
        } catch (Exception e) {
            log.error("getConsumeConfig " + e.toString(), e);
        }
        return props;
    }
    
    public static Properties getProduceConfig(String name) {
        Properties props = new Properties();
        try {
            Set<String> setName = ProducerConfig.configNames();
            for (String pname : setName) {
                String nkey = name + PRODUCER_PREFIX + pname;
                Object nvalue = NConfig.getConfig().containsKey(nkey) ? NConfig.getConfig().getProperty(nkey) : null;
                if (nvalue != null) {
                    props.put(pname, nvalue);
                }
            }
            props.put(ProducerConfig.CLIENT_ID_CONFIG, name + "_producer_" + UUID.randomUUID().toString());
        } catch (Exception e) {
            log.error("getProduceConfig " + e.toString(), e);
        }
        return props;
    }
    
    public static Properties getStreamConfig(String name) {
        Properties props = new Properties();
        try {
            Set<String> setName = StreamsConfig.configDef().names();
            for (String sname : setName) {
                String nkey = name + STREAM_PREFIX + sname;
                Object nvalue = NConfig.getConfig().containsKey(nkey) ? NConfig.getConfig().getProperty(nkey) : null;
                if (nvalue != null) {
                    props.put(sname, nvalue);
                }
            }
            props.put(StreamsConfig.CLIENT_ID_CONFIG, name + "_stream_" + UUID.randomUUID().toString());
        } catch (Exception e) {
            log.error("getStreamConfig " + e.toString(), e);
        }
        return props;
    }
    
    public static void main(String[] args) {
        try {
            //test1();
            
            //test2();
            
            //test3();
            
            //test4();
            
            //test5();
            
            test6();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void test6() {
        try {
            Properties props = getStreamConfig("worker");
            System.out.println(props.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
    application.id
    bootstrap.servers
    replication.factor
    state.dir
    cache.max.bytes.buffering
    client.id
    default.deserialization.exception.handler
    default.key.serde
    default.production.exception.handler
    default.timestamp.extractor
    default.value.serde
    num.standby.replicas
    num.stream.threads
    max.task.idle.ms
    processing.guarantee
    security.protocol
    topology.optimization
    application.server
    buffered.records.per.partition
    built.in.metrics.version
    commit.interval.ms
    connections.max.idle.ms
    metadata.max.age.ms
    metrics.num.samples
    metric.reporters
    metrics.recording.level
    metrics.sample.window.ms
    partition.grouper
    poll.ms
    receive.buffer.bytes
    reconnect.backoff.ms
    reconnect.backoff.max.ms
    retries
    retry.backoff.ms
    request.timeout.ms
    rocksdb.config.setter
    send.buffer.bytes
    state.cleanup.delay.ms
    upgrade.from
    windowstore.changelog.additional.retention.ms
     */
    private static void test5() {
        try {
            Set<String> setName = StreamsConfig.configDef().names();
            for (String sname : setName) {
                System.out.println(sname);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void test4() {
        try {
            Properties props = getProduceConfig("worker");
            System.out.println(props.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
    bootstrap.servers
    client.dns.lookup
    buffer.memory
    retries
    acks
    compression.type
    batch.size
    linger.ms
    delivery.timeout.ms
    client.id
    send.buffer.bytes
    receive.buffer.bytes
    max.request.size
    reconnect.backoff.ms
    reconnect.backoff.max.ms
    retry.backoff.ms
    max.block.ms
    request.timeout.ms
    metadata.max.age.ms
    metadata.max.idle.ms
    metrics.sample.window.ms
    metrics.num.samples
    metrics.recording.level
    metric.reporters
    max.in.flight.requests.per.connection
    key.serializer
    value.serializer
    connections.max.idle.ms
    partitioner.class
    interceptor.classes
    security.protocol
    security.providers
    ssl.protocol
    ssl.provider
    ssl.cipher.suites
    ssl.enabled.protocols
    ssl.keystore.type
    ssl.keystore.location
    ssl.keystore.password
    ssl.key.password
    ssl.truststore.type
    ssl.truststore.location
    ssl.truststore.password
    ssl.keymanager.algorithm
    ssl.trustmanager.algorithm
    ssl.endpoint.identification.algorithm
    ssl.secure.random.implementation
    sasl.kerberos.service.name
    sasl.kerberos.kinit.cmd
    sasl.kerberos.ticket.renew.window.factor
    sasl.kerberos.ticket.renew.jitter
    sasl.kerberos.min.time.before.relogin
    sasl.login.refresh.window.factor
    sasl.login.refresh.window.jitter
    sasl.login.refresh.min.period.seconds
    sasl.login.refresh.buffer.seconds
    sasl.mechanism
    sasl.jaas.config
    sasl.client.callback.handler.class
    sasl.login.callback.handler.class
    sasl.login.class
    enable.idempotence
    transaction.timeout.ms
    transactional.id
     */
    private static void test3() {
        try {
            Set<String> setName = ProducerConfig.configNames();
            for (String pname : setName) {
                System.out.println(pname);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void test2() {
        try {
            Properties props = getConsumeConfig("worker");
            System.out.println(props.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
    bootstrap.servers
    client.dns.lookup
    group.id
    group.instance.id
    session.timeout.ms
    heartbeat.interval.ms
    partition.assignment.strategy
    metadata.max.age.ms
    enable.auto.commit
    auto.commit.interval.ms
    client.id
    client.rack
    max.partition.fetch.bytes
    send.buffer.bytes
    receive.buffer.bytes
    fetch.min.bytes
    fetch.max.bytes
    fetch.max.wait.ms
    reconnect.backoff.ms
    reconnect.backoff.max.ms
    retry.backoff.ms
    auto.offset.reset
    check.crcs
    metrics.sample.window.ms
    metrics.num.samples
    metrics.recording.level
    metric.reporters
    key.deserializer
    value.deserializer
    request.timeout.ms
    default.api.timeout.ms
    connections.max.idle.ms
    interceptor.classes
    max.poll.records
    max.poll.interval.ms
    exclude.internal.topics
    internal.leave.group.on.close
    isolation.level
    allow.auto.create.topics
    security.providers
    security.protocol
    ssl.protocol
    ssl.provider
    ssl.cipher.suites
    ssl.enabled.protocols
    ssl.keystore.type
    ssl.keystore.location
    ssl.keystore.password
    ssl.key.password
    ssl.truststore.type
    ssl.truststore.location
    ssl.truststore.password
    ssl.keymanager.algorithm
    ssl.trustmanager.algorithm
    ssl.endpoint.identification.algorithm
    ssl.secure.random.implementation
    sasl.kerberos.service.name
    sasl.kerberos.kinit.cmd
    sasl.kerberos.ticket.renew.window.factor
    sasl.kerberos.ticket.renew.jitter
    sasl.kerberos.min.time.before.relogin
    sasl.login.refresh.window.factor
    sasl.login.refresh.window.jitter
    sasl.login.refresh.min.period.seconds
    sasl.login.refresh.buffer.seconds
    sasl.mechanism
    sasl.jaas.config
    sasl.client.callback.handler.class
    sasl.login.callback.handler.class
    sasl.login.class
     */
    private static void test1() {
        try {
            Set<String> setName = ConsumerConfig.configNames();
            //System.out.println("setName: " + setName);
            for (String name : setName) {
                System.out.println(name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
