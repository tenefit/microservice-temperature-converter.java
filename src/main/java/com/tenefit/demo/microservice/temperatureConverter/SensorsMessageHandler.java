/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
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
    public enum TemperatureUnit
    {
        C
        {
            public int toCelsius(
                int value)
            {
                return value;
            }

            public int toFahrenheit(
                int value)
            {
                return (int)Math.round(value / 5.0 * 9.0 + 32.0);
            }

            public int toKelvin(
                int value)
            {
                return (int)Math.round(value + 273.2);
            }
        },

        F
        {
            public int toCelsius(
                int value)
            {
                return (int)Math.round((value - 32.0) / 9.0 * 5.0);
            }

            public int toFahrenheit(
                int value)
            {
                return value;
            }

            public int toKelvin(
                int value)
            {
                return (int)Math.round((value + 459.7) / 9.0 * 5.0);
            }
        },

        K
        {
            public int toCelsius(
                int value)
            {
                return (int)Math.round(value - 273.2);
            }

            public int toFahrenheit(
                int value)
            {
                return (int)Math.round(value / 5.0 * 9.0 - 459.7);
            }

            public int toKelvin(
                int value)
            {
                return value;
            }
        };

        public abstract int toCelsius(
            int value);

        public abstract int toFahrenheit(
            int value);

        public abstract int toKelvin(
            int value);
    }

    private final String outboundTopic;

    private final Producer<String, String> producer;

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

    public static int convertTemperature(
        int temperature,
        TemperatureUnit fromUnit,
        TemperatureUnit toUnit)
    {
        switch (toUnit)
        {
        case C:
            return fromUnit.toCelsius(temperature);
        case F:
            return fromUnit.toFahrenheit(temperature);
        default:
            return fromUnit.toKelvin(temperature);
        }
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
            convertTemperature(valueAsJson.getAsInt(), inboundUnit, currentUnit));
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outboundTopic, record.key(), readingsMessage);
        producerRecord.headers().add("row", rowHeader.get().value());
        producer.send(producerRecord).get();
    }

}
