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

import com.ntc.kafka.producer.KProducerUtil;
import java.util.*;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author nghiatc
 * @since Jun 9, 2020
 */
public class WordCountProcessorProducer {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            String name = "wordcountprocessor";
            String topic = "streams-plaintext-input";
            List<String> msgs = new ArrayList<>();
            msgs.add("all streams lead to kafka");
            msgs.add("hello kafka streams");
            msgs.add("join kafka summit");
            
            for (String line : msgs) {
                Future<RecordMetadata> ft = KProducerUtil.sendRecordBytes(name, topic, line);
                Thread.sleep(500);
            }
            Thread.sleep(2000);
            System.out.println("WordCountProcessorProducer End...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
