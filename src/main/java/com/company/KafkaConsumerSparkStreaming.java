package com.company;

/**
 * Created by sepidehayani on 2/14/16.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

        import java.util.*;

        import kafka.serializer.StringDecoder;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.function.Function;
        import org.apache.spark.api.java.function.Function2;
        import org.apache.spark.api.java.function.PairFunction;
        import org.apache.spark.streaming.Duration;
        import org.apache.spark.streaming.api.java.*;

        import org.apache.spark.streaming.kafka.KafkaUtils;
        import scala.Tuple2;

  public class KafkaConsumerSparkStreaming {
      public static void main(String[] args) throws Exception {

    if (args.length < 3) {
      System.err.println("Usage: KafkaConsumer <brokers> <topics> <numThreads>");
      System.exit(1);
    }
    //Defining properties
    Properties props = new Properties();
    props.put("zookeeper.hosts", "10.252.1.136");
    props.put("zookeeper.port", "2181");
    props.put("zookeeper.broker.path", "/brokers");
    props.put("kafka.topic", "test-topic");
    props.put("kafka.consumer.id", "test-id");
    props.put("zookeeper.consumer.connection", "10.252.5.113:2182");
    props.put("zookeeper.consumer.path", "/spark-kafka");
    // Optional Properties
    props.put("consumer.forcefromstart", "true");
    props.put("consumer.fetchsizebytes", "1048576");
    props.put("consumer.fillfreqms", "250");
    props.put("consumer.backpressure.enabled", "true");

    String brokers = args[0];

    //Configure the Streaming Context
    SparkConf sparkConf = new SparkConf().setAppName("KafkaConsumer");
    //Create the context with 2 seconds batch size
    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(10000));

    int numThreads = Integer.parseInt(args[2]);

    final Map<String, Integer> topicMap = new HashMap<String, Integer>();
    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);

    String[] topics = args[1].split(",");
    for (String topic: topics) {topicMap.put(topic, numThreads);
    }

      JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
              jsc,
              String.class,
              String.class,
              StringDecoder.class,
              StringDecoder.class,
              kafkaParams,
              new HashSet<String>()
      );

      JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2().split(" ") [0];
      }
    });

      JavaPairDStream<String, Integer> KafkaConsumer = lines.mapToPair(
            new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String one) {
        return new Tuple2<String, Integer>(one, 1);
              }

            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer T1, Integer T2) {
        return T1 + T2;
      }
    });
    KafkaConsumer.print();

     // Start the computation
    jsc.start();
    jsc.awaitTermination();
  }
  }

