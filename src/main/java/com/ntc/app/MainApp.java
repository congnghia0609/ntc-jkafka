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

/**
 *
 * @author nghiatc
 * @since Jun 8, 2020
 */
public class MainApp {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            //// Lab 1: Kafka Queue & Pub-Sub
            //EmailService es = new EmailService(1);
            //es.start();
            
            
            //// Lab 2: Kafka Streams
//            /** Example Kafka Streams Start **/
//            WordCountConsumer wcc = new WordCountConsumer(1);
//            wcc.start();
//
//            /**
//             * Để chạy Streams thành công ta phải tạo các topic bằng tay trước, với các lệnh sau:
//             * 
//             * == Step 3: Prepare input topic and start Kafka producer
//             * 
//             * # Create the input topic named "streams-plaintext-input" and the output topic named "streams-wordcount-output":
//             * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input
//             * 
//             * # Create the output topic with "compaction" enabled because the output stream is a changelog stream
//             * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-wordcount-output --config cleanup.policy=compact
//             */
//            WordCountStream wcs = new WordCountStream();
//            wcs.start();
//            /** Example Kafka Streams End **/
            
            
            //// Lab 3: Kafka Streams Processor
            /** Example Kafka Streams Processor Start **/
            WordCountProcessorConsumer wcpc = new WordCountProcessorConsumer(1);
            wcpc.start();
            
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
            WordCountProcessor wcp = new WordCountProcessor();
            wcp.start();
            /** Example Kafka Streams Processor End **/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
