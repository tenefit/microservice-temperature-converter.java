/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tenefit.demo.microservice.temperatureConverter.TempUtils.TemperatureUnit;

public class SensorsMessageHandler
{
    String outboundTopic;

    private KafkaProducer<String, String> producer;

    private final Gson gson;

    public SensorsMessageHandler(
        String outboundTopic,
        final KafkaProducerFactory kafkaProducerFactory,
        final Properties kafkaProducerOptions)
    {
        this.outboundTopic = outboundTopic;

        producer = kafkaProducerFactory.newKafkaProducer(kafkaProducerOptions);

        gson = new Gson();
    }

    public void handleMessage(
        ConsumerRecord<String, String> record,
        TemperatureUnit currentTempUnit) throws InterruptedException, ExecutionException
    {
        JsonObject message = gson.fromJson(record.value(), JsonObject.class);

        final Header[] headers = record.headers().toArray();
        Optional<Header> rowHeader = Arrays.stream(headers).filter(h -> h.key().equals("row")).findFirst();
        if (!rowHeader.isPresent())
        {
            return;
        }

        JsonElement unitEl = message.get("unit");
        if (unitEl == null)
        {
            return;
        }
        TemperatureUnit inboundTempUnit = TemperatureUnit.valueOf(unitEl.getAsString());
        JsonElement valueEl = message.get("value");
        if (valueEl == null)
        {
            return;
        }
        // TODO Kosher to build up the JSON message manually as a string? Or should I use GSON
        // to build the object and generate the string?
        String readingsMessage = String.format("{\"id\": \"%s\", \"unit\": \"%s\", \"value\": %d}",
            record.key(),
            currentTempUnit,
            TempUtils.convertTemp(valueEl.getAsInt(), inboundTempUnit, currentTempUnit));
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outboundTopic, record.key(), readingsMessage);
        producerRecord.headers().add("row", rowHeader.get().value());
        producer.send(producerRecord).get();
    }

}
