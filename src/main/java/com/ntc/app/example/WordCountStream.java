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
package com.ntc.app.example;

import com.ntc.kafka.util.KConfig;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

/**
 *
 * @author nghiatc
 * @since Jun 9, 2020
 */
public class WordCountStream {

    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";
    private KafkaStreams streams;
    private CountDownLatch latch = new CountDownLatch(1);

    public Properties getStreamsConfig() {
        Properties props = KConfig.getStreamConfig("wordcount");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public void createWordCountStream(StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(INPUT_TOPIC);
        KTable<String, Long> counts = source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault())))
                .groupBy((key, value) -> value).count();
        // need to override value serde to Long type
        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public void start() {
        Properties props = getStreamsConfig();
        System.out.println("$$$$$" + props.toString());
        StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        streams = new KafkaStreams(builder.build(), props);
        
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook"){
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
