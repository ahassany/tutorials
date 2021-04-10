# Example Spring Boot Application running with docker compose for dev and integration testing

This simple example demonstrates connecting a simple Spring Boot application to a Neo4j instance via docker compose. We
use two different docker compose specs: one for dev and another to run the integration tests.

## Running the dev application

To run the application:

```
./gradlew :bootRun
```

It will automatically start neo4j container (if it's not started already).

If you want to manually start or shutdown the dev containers:

```
./gradlew :devComposeUp
./gradlew :devComposeDown
```

## Running the integration tests

```
./gradlew :integrationTest
```
