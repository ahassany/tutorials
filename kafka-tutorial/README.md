# Simple Kafka demos

Demos are largely based on Confluent's tutorials: https://kafka-tutorials.confluent.io/

## Demo 1: Kafka Producer with basic Kafka API
Just simple application that reads from a string file and push to Kafka topic.
The application is configurable with `src/main/resources/app.properties`.

To run the application use: `./gradlew :producer:run`


## Demo 2: Kafka Consumer with basic Kafka API
Just simple application that reads from a Kafka topic and prints the messages to stdout.
The application is configurable with `src/main/resources/app.properties`.

To run the application use: `./gradlew :consumer:run`.

*Notes* 
- Make sure to run the producer first. This will load the needed Docker image and push some messages to a Kafka topic.
- Make sure `src/main/resources/app.properties` in producer and consumer apps are pointing to the same topic.
