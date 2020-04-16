# Temperature Converter Microservice

A microservice that receives temperature readings from a Kafka topic, converts them to the target temperature unit, and publishes the result to another Kafka topic.

The service also subscribes to a command topic to remote control the service and designate the target temperature unit (**C**elsius, **F**ahrenheit, or **K**elvin).

### Receiving original sensor readings

The microservice receives sensor readings in order to convert the temperatures and re-publish them (see [Publishing converted sensor readings](publishing-converted-sensor-readings#))

- **topic:** `sensors` (specified with `--input-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | row | 9 |
- **payload:**
  ```json
  { "id": "4", "unit": "C", "value": 100 }
  ```

### Publishing converted sensor readings

After receiving sensor readings (see [Receiving original sensor readings](#receiving-original-sensor-readings)), the temperature is converted to the target temperature unit and re-published.

- **topic:** `readings` (specified with `--output-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | row | 9 |
- **payload:**
  ```json
  { "id": "4", "unit": "F", "value": 212 }
  ```

### Receiving control messages

The microservice can be controlled to changed the target temperature unit. Upon receiving a control message the microservice will change its behavior accordingly, and send a response as a confirmation. The response topic used will be that from the `$http.replyTo` header, and the response message will include a `$http.correlationId` header with the same value as that received.

- **topic:** `readings.requests` (specified with `--requests-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | $http.replyTo | readings.responses |
  | $http.correlationId | 123 |
- **payload:**
  ```json
  { "unit": "F" }
  ```

## Build

```
$ mvn clean install
```

## Run

```
$ java -jar target/microservice-temperature-converter-develop-SNAPSHOT.jar \
  -b kafka.tenefit.cloud:9092 \
  --input-topic sensors \
  --output-topic readings \
  --requests-topic readings.requests
```

Additional Kafka consumer and producer properties can be specified if needed:

```
$ java -jar target/microservice-temperature-converter-develop-SNAPSHOT.jar \
  -b kafka.tenefit.cloud:9092 \
  --input-topic sensors \
  --output-topic readings \
  --requests-topic readings.requests \
  --kafka-consumer-property session.timeout.ms=5000 \
  --kafka-consumer-property ... \
  --kafka-producer-property batch.size=16384 \
  --kafka-producer-property ...
```
