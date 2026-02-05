package com.example.demo.service;

import com.example.demo.model.Car;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KStreamProcessor {
    @Value("${spring.kafka.topic.texassales}")
    private String texassalestopic;
    public void process(KStream<String, Car> stream){
        stream.filter(new Predicate<String, Car>() {
            @Override
            public boolean test(String s, Car car) {
                if(car != null && car.getState()!=null && car.getState().trim().equalsIgnoreCase("TEXAS")){
                    return true;
                }
                return false;
            }
        }).to(texassalestopic);
    }
}
