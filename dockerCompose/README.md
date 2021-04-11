# Example Spring Boot Application running with docker compose for dev and integration testing

This simple example demonstrates connecting a simple Spring Boot application to a Neo4j instance via docker compose. We
use two different docker compose specs: one for dev and another to run the integration tests.
See the [blog post](https://ahassany.medium.com/automatically-launch-docker-compose-from-gradle-for-dev-and-integration-tests-4a63b78ad7a1) for more details.

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
