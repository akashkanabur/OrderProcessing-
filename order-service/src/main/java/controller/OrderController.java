package com.example.order_service.controller;

import com.example.order_service.config.KafkaProducerConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/order")
public class OrderController {

    private final KafkaProducerConfig kafkaProducer;

    public OrderController(KafkaProducerConfig kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/placeOrder")
    public ResponseEntity<String> placeOrder(@RequestBody String orderDetails) {
        kafkaProducer.sendMessage("order_topic", orderDetails);
        return ResponseEntity.ok("Order placed successfully!");
    }
}