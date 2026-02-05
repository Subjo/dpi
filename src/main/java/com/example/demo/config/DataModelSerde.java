package com.example.demo.config;

import com.example.demo.model.Car;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class DataModelSerde extends Serdes.WrapperSerde<Car> {
    public DataModelSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(Car.class));

    }

}
