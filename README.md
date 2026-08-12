# Real-Time Order Processing System

A microservices-based, event-driven order processing system built using **Java, Spring Boot, Apache Kafka, and Docker**.

The system demonstrates asynchronous communication between independent microservices using Kafka. When an order is placed, the Order Service publishes an event to Kafka, which is independently consumed by the Notification and Payment services.

---

## 🚀 Architecture

```text
                         Client
                           |
                           | POST /api/order/placeOrder
                           v
                  +-------------------+
                  |   Order Service   |
                  |      :8081        |
                  +---------+---------+
                            |
                            | KafkaTemplate
                            | Producer
                            v
                     +-------------+
                     |    Kafka    |
                     | order_topic |
                     +------+------+
                            |
                 +----------+----------+
                 |                     |
                 v                     v
        +----------------+    +----------------+
        | Notification   |    |    Payment     |
        |    Service     |    |    Service     |
        |     :8082      |    |     :8083      |
        +-------+--------+    +-------+--------+
                |                     |
                v                     v
        notification_group      payment_group
                |                     |
                v                     v
        Notification           Payment Processing
         Processing

```

📌 Project Overview

The system consists of three independent Spring Boot microservices:

1. Order Service

Responsible for receiving order requests through a REST API and publishing order events to Kafka.

Port: 8081

Flow:
```text

HTTP Request
     ↓
OrderController
     ↓
KafkaTemplate
     ↓
Kafka
     ↓
order_topic
```

2. Notification Service

Consumes order events from Kafka and processes notification-related actions.

Port: 8082

Flow:

Kafka
  ↓
order_topic
  ↓
@KafkaListener
  ↓
OrderConsumer
  ↓
Notification Processing
3. Payment Service

Consumes the same order events independently and performs payment processing.

Port: 8083

Flow:

Kafka
  ↓
order_topic
  ↓
@KafkaListener
  ↓
PaymentConsumer
  ↓
Payment Processing
🛠️ Tech Stack
Technology	Purpose
Java 21	Programming language
Spring Boot	Microservice development
Spring Web	REST APIs
Spring Kafka	Kafka producer and consumer integration
Apache Kafka	Event streaming and asynchronous communication
ZooKeeper	Kafka coordination
Docker	Kafka infrastructure containerization
Maven	Build and dependency management
🏗️ Project Structure
OrderProcessing/
│
├── order-service/
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── java/
│           │   └── com.example.order_service/
│           │       ├── OrderServiceApplication.java
│           │       ├── config/
│           │       │   └── KafkaProducerConfig.java
│           │       └── controller/
│           │           └── OrderController.java
│           │
│           └── resources/
│               └── application.properties
│
├── notification-service/
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── java/
│           │   └── com.example.notification_service/
│           │       ├── NotificationAppApplication.java
│           │       └── consumer/
│           │           └── OrderConsumer.java
│           │
│           └── resources/
│               └── application.properties
│
├── payment-service/
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── java/
│           │   └── com.example.payment_service/
│           │       ├── PaymentServiceAppApplication.java
│           │       └── consumer/
│           │           └── PaymentConsumer.java
│           │
│           └── resources/
│               └── application.properties
│
├── docker-compose.yml
└── README.md
🔄 Event Flow

When a customer places an order:

Step 1 — Client sends request
POST http://localhost:8081/api/order/placeOrder

Example request:

{
  "orderId": "ORD001",
  "product": "Laptop",
  "quantity": 1
}
Step 2 — Order Service receives the request

OrderController handles the HTTP request.

@PostMapping("/placeOrder")
public ResponseEntity<String> placeOrder(
        @RequestBody String orderDetails) {

    kafkaProducer.sendMessage(
            "order_topic",
            orderDetails
    );

    return ResponseEntity.ok(
            "Order placed successfully!"
    );
}
Step 3 — Order event is published to Kafka

The Order Service uses Spring Kafka's KafkaTemplate.

Order Service
      |
      v
KafkaTemplate
      |
      v
order_topic
Step 4 — Notification Service consumes the event

The Notification Service listens to:

order_topic

using:

@KafkaListener(
    topics = "order_topic",
    groupId = "notification_group"
)
Step 5 — Payment Service consumes the same event

The Payment Service uses a different consumer group:

@KafkaListener(
    topics = "order_topic",
    groupId = "payment_group"
)

Therefore, both services independently receive the order event.

                    order_topic
                         |
                 +-------+-------+
                 |               |
                 v               v
       notification_group   payment_group
                 |               |
                 v               v
          Notification        Payment
             Service           Service
🔑 Kafka Concepts Demonstrated
Producer

The Order Service acts as the Kafka producer.

It uses:

KafkaTemplate<String, String>

to publish messages.

Consumer

Notification and Payment services act as Kafka consumers.

They use:

@KafkaListener

to consume messages.

Kafka Topic

The project uses:

order_topic

to transport order events.

Consumer Groups

Two independent consumer groups are used:

notification_group
payment_group

Using different groups allows both services to independently consume the same order event.

🐳 Docker Infrastructure

Kafka and ZooKeeper are run using Docker.

Start the infrastructure with:

docker compose up -d

Check running containers:

docker ps

Expected containers:

order-kafka
order-zookeeper

Kafka is exposed on:

localhost:9092

ZooKeeper is exposed on:

localhost:2181
⚙️ Configuration
Order Service
spring.application.name=OrderServiceApp
server.port=8081

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
Notification Service
spring.application.name=NotificationApp
server.port=8082

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=notification_group
spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
Payment Service
spring.application.name=PaymentService
server.port=8083

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=payment_group
spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
▶️ How to Run
1. Start Kafka and ZooKeeper

From the directory containing docker-compose.yml:

docker compose up -d

Verify:

docker ps
2. Start Order Service

Run:

OrderServiceApplication

Expected:

Tomcat started on port 8081
3. Start Notification Service

Run:

NotificationAppApplication

Expected:

Tomcat started on port 8082
4. Start Payment Service

Run:

PaymentServiceAppApplication

Expected:

Tomcat started on port 8083
🧪 Testing

Once Kafka and all three services are running, send an order.

Endpoint
POST http://localhost:8081/api/order/placeOrder
Request Body
{
  "orderId": "ORD001",
  "product": "Laptop",
  "quantity": 1
}
Expected Response
Order placed successfully!
Expected Notification Output

The Notification Service should receive the event:

Order Received: {"orderId":"ORD001","product":"Laptop","quantity":1}
Expected Payment Output

The Payment Service should receive the same event:

Payment Service - Received order: {"orderId":"ORD001","product":"Laptop","quantity":1}

Payment processed successfully!
📊 End-to-End Flow
                         POST Request
                              |
                              v
                    +-------------------+
                    |   Order Service   |
                    |      :8081        |
                    +---------+---------+
                              |
                              | Publish Event
                              v
                       +-------------+
                       |    Kafka    |
                       | order_topic |
                       +------+------+
                              |
                    +---------+---------+
                    |                   |
                    v                   v
          +----------------+   +----------------+
          | Notification   |   |    Payment     |
          |    Service     |   |    Service     |
          |     :8082      |   |     :8083      |
          +----------------+   +----------------+
                    |                   |
                    v                   v
              Notification        Payment
               Processing         Processing
🎯 Key Features
Microservices-based architecture
RESTful API for order placement
Event-driven architecture
Asynchronous communication using Apache Kafka
Kafka producer implementation using KafkaTemplate
Kafka consumer implementation using @KafkaListener
Independent consumer groups for different services
Dockerized Kafka and ZooKeeper infrastructure
Independent deployment and execution of services
Loose coupling between Order, Notification, and Payment services
💡 Why Kafka?

Kafka allows the Order Service to publish an event without directly depending on the Notification or Payment services.

Instead of:

Order Service
      |
      +----> Payment Service
      |
      +----> Notification Service

the system uses:

Order Service
      |
      v
    Kafka
    /   \
   v     v
Payment Notification

This provides:

Loose coupling
Asynchronous processing
Independent service scaling
Event-driven communication
Better separation of responsibilities
📚 Concepts Demonstrated

This project demonstrates practical understanding of:

Spring Boot
REST APIs
Dependency Injection
Spring Beans
Constructor Injection
Kafka Producers
Kafka Consumers
Kafka Topics
Consumer Groups
Event-driven architecture
Microservices
Asynchronous communication
Docker
Maven
