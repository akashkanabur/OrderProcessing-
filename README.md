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
```text

Kafka
  ↓
order_topic
  ↓
@KafkaListener
  ↓
OrderConsumer
  ↓
Notification Processing
```
3. Payment Service

Consumes the same order events independently and performs payment processing.

Port: 8083

Flow:
```text
Kafka
  ↓
order_topic
  ↓
@KafkaListener
  ↓
PaymentConsumer
  ↓
Payment Processing
```
##🛠️ Tech Stack
Technology	Purpose
Java 21	Programming language
Spring Boot	Microservice development
Spring Web	REST APIs
Spring Kafka	Kafka producer and consumer integration
Apache Kafka	Event streaming and asynchronous communication
ZooKeeper	Kafka coordination
Docker	Kafka infrastructure containerization
Maven	Build and dependency management

##🏗️ Project Structure
```bash
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
```
🔄 Event Flow

When a customer places an order:

Step 1 — Client sends request
POST http://localhost:8081/api/order/placeOrder

Example request:
```bash
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
```
Step 3 — Order event is published to Kafka

The Order Service uses Spring Kafka's KafkaTemplate.
```text
Order Service
      |
      v
KafkaTemplate
      |
      v
order_topic
```
Step 4 — Notification Service consumes the event

The Notification Service listens to:

order_topic

using:
```text
@KafkaListener(
    topics = "order_topic",
    groupId = "notification_group"
)
```
Step 5 — Payment Service consumes the same event

The Payment Service uses a different consumer group:
```text
@KafkaListener(
    topics = "order_topic",
    groupId = "payment_group"
)
```
Therefore, both services independently receive the order event.
```text
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
```

▶️ How to Run
1. Start Kafka and ZooKeeper

From the directory containing docker-compose.yml:
```
docker compose up -d
```
Verify:
```
docker ps
```
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
```
POST http://localhost:8081/api/order/placeOrder
Request Body
{
  "orderId": "ORD001",
  "product": "Laptop",
  "quantity": 1
}
```
Expected Response
Order placed successfully!

Expected Notification Output -- The Notification Service should receive the event:

Order Received: 
```
{"orderId":"ORD001","product":"Laptop","quantity":1}
```

Expected Payment Output -- The Payment Service should receive the same event:

Payment Service - Received order:
```
{"orderId":"ORD001","product":"Laptop","quantity":1}
```
Payment processed successfully!

📊 End-to-End Flow
```
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
```
