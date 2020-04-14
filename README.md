# Temperature Converter Microservice

A microservice that receives temperature readings from a Kafka topic, converts them to the target temperature unit, and publishes the result to another Kafka topic.

The service also subscribes to a command topic to remote control the service and designate the target temperature unit (**C**elsius, **F**ahrenheit, or **K**elvin).

### Receiving original sensor readings

The microservice receives sensor readings in order to convert the temperatures and re-publish them (see [Publishing converted sensor readings](publishing-converted-sensor-readings#))

- **topic:** `sensors` (specified with `--sensors-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | row | 9 |
- **payload:**
  ```json
  { "id": "4", "unit": "C", "value": "100" }
  ```

### Publishing converted sensor readings

After receiving sensor readings (see [Receiving original sensor readings](#receiving-original-sensor-readings)), the temperature is converted to the target temperature unit and re-published.

- **topic:** `readings` (specified with `--readings-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | row | 9 |
- **payload:**
  ```json
  { "id": "4", "unit": "F", "value": "212" }
  ```

### Receiving control messages

The microservice can be controlled to changed the target temperature unit.

- **topic:** `readings.requests` (specified with `--readings-requests-topic`)
- **headers:** none
- **payload:**
  ```json
  { "unit": "F" }
  ```

### Responding to control messages

Upon receiving a control message (see [Receiving control messages](#receiving-control-messages)) the microservice will change its behavior accordingly, and send a response as a confirmation.

- **topic:** `readings.responses` (specified with `--readings-responses-topic`)
- **headers:** none
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
  --sensors-topic sensors \
  --readings-topic readings \
  --readings-requests-topic readings.requests \
  --readings-responses-topic readings.responses
```

Additional Kafka consumer and producer properties can be specified if needed:

```
$ java -jar target/microservice-temperature-converter-develop-SNAPSHOT.jar \
  -b kafka.tenefit.cloud:9092 \
  --sensors-topic sensors \
  --readings-topic readings \
  --readings-requests-topic readings.requests \
  --readings-responses-topic readings.responses \
  --kafka-consumer-property session.timeout.ms=5000 \
  --kafka-consumer-property ... \
  --kafka-producer-property batch.size=16384 \
  --kafka-producer-property ...
```
