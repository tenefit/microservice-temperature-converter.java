/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tenefit.demo.microservice.temperatureConverter.SensorsMessageHandler.TemperatureUnit;

public class ReadingsMessageHandler
{
    private final String outboundTopic;

    private final Producer<String, String> producer;

    private final Gson gson;

    public ReadingsMessageHandler(
        String outboundTopic,
        final KafkaProducerFactory kafkaProducerFactory,
        final Properties kafkaProducerOptions)
    {
        this.outboundTopic = outboundTopic;

        producer = kafkaProducerFactory.newKafkaProducer(kafkaProducerOptions);

        gson = new Gson();
    }

    public TemperatureUnit handleMessage(
        ConsumerRecord<String, String> record) throws InterruptedException, ExecutionException
    {
        JsonObject message = gson.fromJson(record.value(), JsonObject.class);
        JsonElement unit = message.get("unit");
        if (unit == null)
        {
            return null;
        }
        TemperatureUnit inboundTempUnit = TemperatureUnit.valueOf(unit.getAsString());
        String responseMessage = String.format("{\"unit\": \"%s\"}", inboundTempUnit);
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outboundTopic, null, responseMessage);
        producer.send(producerRecord).get();
        return inboundTempUnit;
    }

}
