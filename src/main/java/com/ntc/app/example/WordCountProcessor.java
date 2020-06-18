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
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 *
 * @author nghiatc
 * @since Jun 10, 2020
 */
public class WordCountProcessor {
    
    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-processor-output";
    public static final String NAME_SOURCE = "Source";
    public static final String NAME_PROCESS = "Process";
    public static final String NAME_STORE = "Counts";
    public static final String NAME_SINK = "Sink";
    
    private KafkaStreams streams;
    private CountDownLatch latch = new CountDownLatch(1);

    
    public class WCProcessorSuplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;
                
                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(Duration.ofMillis(100), PunctuationType.STREAM_TIME, timestamp -> {
                        try (KeyValueIterator<String, Integer> iter = kvStore.all()) {
                            System.out.println("----------- " + timestamp + " ----------- ");
                            
                            while (iter.hasNext()) {
                                KeyValue<String, Integer> entry = iter.next();
                                System.out.println("[" + entry.key + ", " + entry.value + "]");
                                context.forward(entry.key, entry.value.toString());
                            }
                        }
                    });
                    this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore(NAME_STORE);
                }

                @Override
                public void process(String key, String value) {
                    System.out.println("process value: " + value);
                    String[] words = value.toLowerCase(Locale.getDefault()).split(" ");
                    for (String word : words) {
                        Integer counter = this.kvStore.get(word);
                        if (counter == null) {
                            counter = 1;
                        } else {
                            counter += 1;
                        }
                        this.kvStore.put(word, counter);
                    }
                    context.commit();
                }

                @Override
                public void close() {
                }
            };
        }
        
    }

    public Properties getStreamsConfig() {
        Properties props = KConfig.getStreamConfig("wordcountprocessor");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    
    public void start() {
        Properties props = getStreamsConfig();
        Topology topo = new Topology();
        topo.addSource(NAME_SOURCE, INPUT_TOPIC);
        topo.addProcessor(NAME_PROCESS, new WCProcessorSuplier(), NAME_SOURCE);
        topo.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(NAME_STORE),
                Serdes.String(),
                Serdes.Integer()),
                NAME_PROCESS);
        topo.addSink(NAME_SINK, OUTPUT_TOPIC, NAME_PROCESS);
        
        streams = new KafkaStreams(topo, props);
        
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
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
    
    /**
     * @param args the command line arguments
     * 
     * Để chạy Streams thành công ta phải tạo các topic bằng tay trước, với các lệnh sau:
     * 
     * == Step 3: Prepare input topic and start Kafka producer
     * 
     * # Create the input topic named "streams-plaintext-input" and the output topic named "streams-wordcount-output":
     * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input
     * 
     * # Create the output topic with "compaction" enabled because the output stream is a changelog stream
     * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-wordcount-processor-output --config cleanup.policy=compact
     * 
     */
    public static void main(String[] args) {
        try {
            WordCountProcessor wcp = new WordCountProcessor();
            wcp.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
