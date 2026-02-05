package com.example.demo.controller;


import com.example.demo.model.Car;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
@RequestMapping("/toyotasales")
public class StreamsController {
    private final StreamsBuilderFactoryBean factory;

    public StreamsController(StreamsBuilderFactoryBean factory) {
        this.factory = factory;
    }
    @GetMapping("/dealer/{id}")
    public String toyotasales(@PathVariable String id) {
        KafkaStreams kafkaStreams = factory.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> store = Objects.requireNonNull(kafkaStreams)
                .store(StoreQueryParameters.fromNameAndType("dealer-sales-amount", QueryableStoreTypes.keyValueStore()));
        return "Total Car Sales for Dealer "+id +" is $"+ store.get(id);
    }
}
