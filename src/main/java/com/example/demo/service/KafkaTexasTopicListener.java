package com.example.demo.service;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class KafkaTexasTopicListener {
    @KafkaListener(topics = "${spring.kafka.topic.texassales}")
    public void readRxClaimStream(@Payload String payload) {
        if(payload != null && !payload.isEmpty()) {
            try {
                System.out.println("TEXAS TOPIC => " + payload);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
