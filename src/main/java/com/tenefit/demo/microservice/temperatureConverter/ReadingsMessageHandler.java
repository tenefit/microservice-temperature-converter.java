/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ReadingsMessageHandler
{
    private final Producer<String, String> producer;

    private final Gson gson;

    public ReadingsMessageHandler(
        final KafkaProducerFactory kafkaProducerFactory,
        final Properties kafkaProducerOptions)
    {
        producer = kafkaProducerFactory.newKafkaProducer(kafkaProducerOptions);

        gson = new Gson();
    }

    public TemperatureUnit handleMessage(
        ConsumerRecord<String, String> record) throws InterruptedException, ExecutionException
    {
        Header[] headers = record.headers().toArray();

        List<Header> replyToHeaders = Arrays.stream(headers)
            .filter(h -> h.key().equals("$http.replyTo"))
            .collect(Collectors.toList());
        assert replyToHeaders.size() == 1;
        String replyTo = new String(replyToHeaders.get(0).value());

        List<Header> correlationIdHeaders = Arrays.stream(headers)
            .filter(h -> h.key().equals("$http.correlationId"))
            .collect(Collectors.toList());
        assert correlationIdHeaders.size() == 1;
        String correlationId = new String(correlationIdHeaders.get(0).value());

        JsonObject messageAsJson = gson.fromJson(record.value(), JsonObject.class);

        JsonElement unitAsJson = messageAsJson.get("unit");

        final String unitAsString = unitAsJson != null ? unitAsJson.getAsString() : null;
        final TemperatureUnit unit = unitAsString != null ? TemperatureUnit.valueOf(unitAsString) : null;
        if (unitAsString != null)
        {
            String response = String.format("{\"unit\": \"%s\"}", unitAsString);
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(replyTo, null, response);
            producerRecord.headers().add("$http.correlationId", correlationId.getBytes(UTF_8));
            producer.send(producerRecord).get();
        }

        return unit;
    }

}
