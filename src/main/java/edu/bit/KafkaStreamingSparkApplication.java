package edu.bit;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;


public class StreamingSparkApplication {

    public static void main(String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", new StringDeserializer());
        kafkaParams.put("value.deserializer", new StringDeserializer());
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
    }
}