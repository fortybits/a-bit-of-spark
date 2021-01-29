package edu.bit;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple2;

public class KafkaStreamingSparkApplication {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("Kafka Streaming Application");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("log"); // topic to produce to

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> lines = stream.map(ConsumerRecord::value);

        JavaDStream<String> words = lines
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator());

        JavaPairDStream<String, Long> userRequests = words
                .mapToPair((PairFunction<String, String, Long>) input -> new Tuple2<>(input, 1L))
                .reduceByKey(Long::sum);

        userRequests.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}