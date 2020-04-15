/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SensorsMessageHandler
{
    private final String outboundTopic;

    private final Producer<String, String> producer;

    private final Gson gson;

    public SensorsMessageHandler(
        final Producer<String, String> producer,
        String outboundTopic)
    {
        this.producer = producer;
        this.outboundTopic = outboundTopic;

        gson = new Gson();
    }

    public void handleMessage(
        ConsumerRecord<String, String> record,
        TemperatureUnit currentUnit) throws InterruptedException, ExecutionException
    {
        final Header[] headers = record.headers().toArray();
        Optional<Header> rowHeader = Arrays.stream(headers).filter(h -> h.key().equals("row")).findFirst();
        if (!rowHeader.isPresent())
        {
            return;
        }

        JsonObject messageAsJson = gson.fromJson(record.value(), JsonObject.class);

        JsonElement unitAsJson = messageAsJson.get("unit");
        if (unitAsJson == null)
        {
            return;
        }
        TemperatureUnit inboundUnit = TemperatureUnit.valueOf(unitAsJson.getAsString());
        JsonElement valueAsJson = messageAsJson.get("value");
        if (valueAsJson == null)
        {
            return;
        }
        String readingsMessage = String.format("{\"id\": \"%s\", \"unit\": \"%s\", \"value\": %d}",
            record.key(),
            currentUnit,
            inboundUnit.convertTo(valueAsJson.getAsInt(), currentUnit));
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outboundTopic, record.key(), readingsMessage);
        producerRecord.headers().add("row", rowHeader.get().value());
        producer.send(producerRecord).get();
    }
}
