package com.example.payment_service.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentConsumer {

    @KafkaListener(
            topics = "order_topic",
            groupId = "payment_group"
    )
    public void consumeMessage(String message) {

        System.out.println(
                "Payment Service - Received order: " + message
        );

        System.out.println("Payment processed successfully!");
    }
}