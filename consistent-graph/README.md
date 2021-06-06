# Producing consistent graphs from Kafka streams

This demo application shows how produce consistent state from different types of incoming event streams.

## Case 1: Incoming event with bounded context
In this case we consider a source system (:orders-generator in this case).
The produces a bunch of orders events.
Each even in this case contains the full order info: the customer and the ordered items in each single message.
The orders can be changed.
However, this topic is a Capture-Data-Change (CDC topic), producing full new order each time.
So we have no way on the receiving end to know what have changed, hence we must do a diff with the latest state observed.

More information are in the [Blog post](https://ahassany.medium.com/streaming-integration-events-to-graph-capture-data-change-cdc-events-bb1e528da5f6)


### Running the application

1. Run docker-compose container with all the components.
_Note_ the default Neo4j password in `docker-compose/main/resource/docker-compose.yml` 

```
gradle :docker-compose:composeUp
```

2. Create index in Neo4j
```
CREATE CONSTRAINT BaseIdUnique ON (b:Base) ASSERT b.id IS UNIQUE
```

3. Run the Kafka connector for Neo4J sink
```
curl -v -X POST --data  "@./docker-compose/src/main/resources/neo4j-sink-connect.json" -H "Content-Type: application/json" http://localhost:8083/connectors
```

4. Generate/Modify/Delete order(s)

Create new orders:
```
gradle :orders-generator:run --args 1  # first argument defined the number of orders, default is 10
```

Remove items from existing order:
```
/gradlew :orders-generator:trim --args 1 1 # Only change 1 order by removing 1 item
```

Delete existing order
```
gradle :orders-generator:delete --args 1  # first argument defined the number of orders, default is 1
``` 


5. Run the streaming application
```
gradle :orders-to-graph-stream:run
```

6. Check that orders are correct in Neo4j at `http://localhost:7474`
