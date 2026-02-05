package com.example.demo.config;


import com.example.demo.model.Car;
import com.example.demo.service.KStreamProcessor;
import com.example.demo.service.KTableProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaStreamsConfig {
    @Value(value = "${spring.kafka.bootstrap-server}")
    private String bootstrapServers;
    @Value(value = "${spring.kafka.topic.demo}")
    private String topic;
    @Autowired
    private KTableProcessor kTableProcessor;
    @Autowired
    private KStreamProcessor kStreamProcessor;
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration  kafkaStreamsConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(APPLICATION_ID_CONFIG,"streams-app");
        config.put(BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, DataModelSerde.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return new KafkaStreamsConfiguration(config);
    }
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "demo-1");
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
    @Bean
    public KStream<String, Car> kStream(StreamsBuilder builder) {
        KStream<String, Car> kStream = builder.stream(topic, Consumed.with(Serdes.String(), new DataModelSerde()));

        this.kStreamProcessor.process(kStream);
        this.kTableProcessor.process(kStream);
        return kStream;
    }
}
