/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tenefit.demo.microservice.temperatureConverter.TempUtils.TemperatureUnit;

public class ReadingsMessageHandler
{
    String outboundTopic;

    private KafkaProducer<String, String> producer;

    public ReadingsMessageHandler(
        String outboundTopic,
        final KafkaProducerFactory kafkaProducerFactory,
        final Properties kafkaProducerOptions)
    {
        this.outboundTopic = outboundTopic;

        producer = kafkaProducerFactory.newKafkaProducer(kafkaProducerOptions);
    }

    public TemperatureUnit handleMessage(
        ConsumerRecord<String, String> record) throws InterruptedException, ExecutionException
    {
        JsonObject message = new Gson().fromJson(record.value(), JsonObject.class);
        JsonElement unitEl = message.get("unit");
        if (unitEl == null)
        {
            return null;
        }
        TemperatureUnit inboundTempUnit = TemperatureUnit.valueOf(unitEl.getAsString());
        // TODO Kosher to build up the JSON message manually as a string? Or should I use GSON
        // to build the object and generate the string?
        String responseMessage = String.format("{\"unit\": \"%s\"}", inboundTempUnit);
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outboundTopic, null, responseMessage);
        producer.send(producerRecord).get();
        return inboundTempUnit;
    }

}
