package com.consumer.kafkaconsumer.config;

import com.consumer.kafkaconsumer.model.Employee;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    public static final String TOPIC_STRING = "topicString";
    public static final String TOPIC_JAVA_CLASS = "topicJavaClass";

    public static final String SOURCE_TOPIC = "TextLinesTopic";
    public static final String DEST_TOPIC = "WordsWithCountsTopic";

    /*@Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, "json");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    public ConsumerFactory<String, Employee> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfigs(),
                new StringDeserializer(),
                new JsonDeserializer<>(Employee.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Employee> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Employee> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }*/


    /*@Bean
    @Qualifier("topicString")
    public NewTopic topicString() {
        return TopicBuilder.name(TOPIC_STRING)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    @Qualifier("topicJavaClass")
    public NewTopic topicJavaClass() {
        return TopicBuilder.name(TOPIC_JAVA_CLASS)
                .partitions(1)
                .replicas(1)
                .build();
    }*/

    @KafkaListener(id = "myId1", topics = {TOPIC_STRING, TOPIC_JAVA_CLASS, SOURCE_TOPIC})
    public void listen1(String in) {
        System.out.println(in);
    }

    @KafkaListener(id = "myId2", topics = {DEST_TOPIC})
    public void listen2(ConsumerRecord<String, Long> in) {
        System.out.println(in);
        System.out.println(in.value());
        //in.forEach((k,v) -> System.out.println(k+" ==> "+v));
    }

    /*@KafkaListener(id = "myId3", topics = TOPIC_JAVA_CLASS, containerFactory = "kafkaListenerContainerFactory", beanRef = "kafkaListenerContainerFactory")
    public void listen3(ConsumerRecord<String, Employee> consumerRecord, Acknowledgment ack) {
        //System.out.println(key);
        System.out.println(consumerRecord.value());
    }*/
}
