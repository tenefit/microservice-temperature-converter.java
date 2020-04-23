/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.StringReader;
import java.util.concurrent.ExecutionException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class ReadingsMessageHandler
{
    private final Producer<String, String> producer;

    public ReadingsMessageHandler(
        final Producer<String, String> producer)
    {
        this.producer = producer;
    }

    public TemperatureUnit handleMessage(
        ConsumerRecord<String, String> input) throws InterruptedException, ExecutionException
    {
        Header replyTo = input.headers().lastHeader("$http.replyTo");
        Header correlationId = input.headers().lastHeader("$http.correlationId");
        try (JsonReader inputJson = Json.createReader(new StringReader(input.value())))
        {
            JsonObject request = inputJson.readObject();
            if (replyTo != null && correlationId != null && request.containsKey("unit"))
            {
                TemperatureUnit unit = TemperatureUnit.valueOf(request.getString("unit"));

                String outputTopic = new String(replyTo.value(), UTF_8);
                String response = Json.createObjectBuilder()
                    .add("unit", unit.toString())
                    .build()
                    .toString();

                ProducerRecord<String, String> output = new ProducerRecord<>(outputTopic, null, response);
                output.headers().add(correlationId);
                producer.send(output).get();
                return unit;
            }
        }
        catch (JsonParsingException ex)
        {
            // The payload on the input message was not valid JSON, so skip it
        }
        return null;
    }

}
