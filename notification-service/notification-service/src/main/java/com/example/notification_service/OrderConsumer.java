package com.example.notification_service.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(
            topics = "order_topic",
            groupId = "notification_group"
    )
    public void consumeMessage(String message) {
        System.out.println("Order Received: " + message);
    }
}