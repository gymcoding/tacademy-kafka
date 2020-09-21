package com.tacademy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SimpleMapProducer {
    private static String TOPIC_NAME = "iot.gateway.data.1";
    private static String BOOTSTRAP_SERVERS = "218.156.90.183:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> data = new HashMap<String, Object>() {
            {
                put("deviceName", "T3000");
                put("deviceType", "default");
                put("model", "modelName");
                put("modelValue", "SAMSUNG");
                put("temp", 0);
            }
        };
        for (int index = 0; index < 10; index++) {
            data.put("temp", index + 1);
            try {
                String strData = mapper.writeValueAsString(data);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, strData);
                producer.send(record);
                System.out.println("Send to " + TOPIC_NAME + " | data : " + strData);
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}