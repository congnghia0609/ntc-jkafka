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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ntc.kafka.util.KConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 *
 * @author nghiatc
 * @since Jun 11, 2020
 */
/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful computation,
 * using specific data types (here: JSON POJO; but can also be Avro specific bindings, etc.) for serdes
 * in Kafka Streams.
 *
 * In this example, we join a stream of pageviews (aka clickstreams) that reads from a topic named "streams-pageview-input"
 * with a user profile table that reads from a topic named "streams-userprofile-input", where the data format
 * is JSON string representing a record in the stream or table, to compute the number of pageviews per user region.
 *
 * Before running this example you must create the input topics and the output topic (e.g. via
 * bin/kafka-topics --create ...), and write some data to the input topics (e.g. via
 * bin/kafka-console-producer). Otherwise you won't see any data arriving in the output topic.
 *
 * The inputs for this example are:
 * - Topic: streams-pageview-input
 *   Key Format: (String) USER_ID
 *   Value Format: (JSON) {"_t": "pv", "user": (String USER_ID), "page": (String PAGE_ID), "timestamp": (long ms TIMESTAMP)}
 *
 * - Topic: streams-userprofile-input
 *   Key Format: (String) USER_ID
 *   Value Format: (JSON) {"_t": "up", "region": (String REGION), "timestamp": (long ms TIMESTAMP)}
 *
 * To observe the results, read the output topic (e.g., via bin/kafka-console-consumer)
 * - Topic: streams-pageviewstats-typed-output
 *   Key Format: (JSON) {"_t": "wpvbr", "windowStart": (long ms WINDOW_TIMESTAMP), "region": (String REGION)}
 *   Value Format: (JSON) {"_t": "rc", "count": (long REGION_COUNT), "region": (String REGION)}
 *
 * Note, the "_t" field is necessary to help Jackson identify the correct class for deserialization in the
 * generic {@link JSONSerde}. If you instead specify a specific serde per class, you won't need the extra "_t" field.
 */
public class PageViewProcessor {
    
    public static final String PV_INPUT_TOPIC = "streams-pageview-input";
    public static final String UP_INPUT_TOPIC = "streams-userprofile-input";
    public static final String OUTPUT_TOPIC = "streams-pageviewstats-typed-output";
    
    private KafkaStreams streams;
    private CountDownLatch latch = new CountDownLatch(1);
    
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = PageView.class, name = "pv"),
        @JsonSubTypes.Type(value = UserProfile.class, name = "up"),
        @JsonSubTypes.Type(value = PageViewByRegion.class, name = "pvbr"),
        @JsonSubTypes.Type(value = WindowedPageViewByRegion.class, name = "wpvbr"),
        @JsonSubTypes.Type(value = RegionCount.class, name = "rc"),
    })
    public interface JSONSerdeCompatible {
    }
    
    public class PageView implements JSONSerdeCompatible {
        public String user;
        public String page;
        public Long timestamp;
    }
    
    public class UserProfile implements JSONSerdeCompatible {
        public String region;
        public Long timestamp;
    }
    
    public class PageViewByRegion implements JSONSerdeCompatible {
        public String user;
        public String page;
        public String region;
    }
    
    public class WindowedPageViewByRegion implements JSONSerdeCompatible {
        public long windowStart;
        public String region;
    }
    
    public class RegionCount implements JSONSerdeCompatible {
        public long count;
        public String region;
    }
    
    /**
     * A serde for any class that implements {@link JSONSerdeCompatible}. Note that the classes also need to
     * be registered in the {@code @JsonSubTypes} annotation on {@link JSONSerdeCompatible}.
     *
     * @param <T> The concrete type of the class that gets de/serialized
     */
    public class JSONSerde<T extends JSONSerdeCompatible> implements Serializer<T>, Deserializer<T>, Serde<T> {

        private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return (T) OBJECT_MAPPER.readValue(data, JSONSerdeCompatible.class);
            } catch (IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
        
        @Override
        public void close() {
        }
        
    }
    
    public Properties getStreamsConfig() {
        Properties props = KConfig.getStreamConfig("pageview");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JSONSerde.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    
    public void start() {
        Properties props = getStreamsConfig();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PageView> views = builder.stream(PV_INPUT_TOPIC, Consumed.with(Serdes.String(), new JSONSerde<>()));
        KTable<String, UserProfile> users = builder.table(UP_INPUT_TOPIC, Consumed.with(Serdes.String(), new JSONSerde<>()));
        KStream<WindowedPageViewByRegion, RegionCount> regionCount = views
                .leftJoin(users, (view, profile) -> {
                    PageViewByRegion viewByRegion = new PageViewByRegion();
                    viewByRegion.user = view.user;
                    viewByRegion.page = view.page;

                    if (profile != null) {
                        viewByRegion.region = profile.region;
                    } else {
                        viewByRegion.region = "UNKNOWN";
                    }
                    return viewByRegion;
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.region, viewRegion))
                .groupByKey(Grouped.with(Serdes.String(), new JSONSerde<>()))
                .windowedBy(TimeWindows.of(Duration.ofDays(7)).advanceBy(Duration.ofSeconds(1)))
                .count()
                .toStream()
                .map((key, value) -> {
                    WindowedPageViewByRegion wViewByRegion = new WindowedPageViewByRegion();
                    wViewByRegion.windowStart = key.window().start();
                    wViewByRegion.region = key.key();

                    RegionCount rCount = new RegionCount();
                    rCount.region = key.key();
                    rCount.count = value;

                    return new KeyValue<>(wViewByRegion, rCount);
                });
        
        // write to the result topic
        regionCount.to(OUTPUT_TOPIC);
        
        streams = new KafkaStreams(builder.build(), props);
        
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        
        try {
            streams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void stop() {
        try {
            if (streams != null) {
                streams.close();
            }
            latch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
