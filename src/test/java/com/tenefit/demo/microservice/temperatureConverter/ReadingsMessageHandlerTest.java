/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tenefit.demo.microservice.temperatureConverter.SensorsMessageHandler.TemperatureUnit;

public class ReadingsMessageHandlerTest
{
    private Gson gson = new Gson();

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveRequestThenRespondWithCorrectMetadata() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"C\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        ReadingsMessageHandler handler = new ReadingsMessageHandler(kafkaProducerFactory, props);
        handler.handleMessage(record);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        assertEquals("readings.responses", sendArg.getValue().topic());
        Header[] headers = sendArg.getValue().headers().toArray();
        assertEquals(1, headers.length);
        List<Header> correlationHeaders = Arrays.stream(headers)
            .filter(h -> h.key().equals("$http.correlationId"))
            .collect(Collectors.toList());
        assertEquals(1, correlationHeaders.size());
        System.out.format("h=%s\n", new String(correlationHeaders.get(0).value()));
        assertArrayEquals("123".getBytes(UTF_8), correlationHeaders.get(0).value());
        assertNull(sendArg.getValue().key());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveCelsiusRequestThenChangeUnitAndRespond() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"C\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        ReadingsMessageHandler handler = new ReadingsMessageHandler(kafkaProducerFactory, props);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.C, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        JsonObject messageAsJson = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(1, messageAsJson.keySet().size());
        JsonElement unitAsJson = messageAsJson.get("unit");
        assertNotNull(unitAsJson);
        assertEquals("C", unitAsJson.getAsString());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveFahrenheitRequestThenChangeUnitAndRespond() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"F\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        ReadingsMessageHandler handler = new ReadingsMessageHandler(kafkaProducerFactory, props);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.F, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        JsonObject messageAsJson = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(1, messageAsJson.keySet().size());
        JsonElement unitAsJson = messageAsJson.get("unit");
        assertNotNull(unitAsJson);
        assertEquals("F", unitAsJson.getAsString());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveKelvinRequestAndThenChangeUnitAndRespond() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"K\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        ReadingsMessageHandler handler = new ReadingsMessageHandler(kafkaProducerFactory, props);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.K, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        JsonObject messageAsJson = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(1, messageAsJson.keySet().size());
        JsonElement unitAsJson = messageAsJson.get("unit");
        assertNotNull(unitAsJson);
        assertEquals("K", unitAsJson.getAsString());
    }
}
