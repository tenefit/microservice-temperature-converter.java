# Temperature Converter Microservice

Build:

```
$ mvn clean install
```

Run:

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
