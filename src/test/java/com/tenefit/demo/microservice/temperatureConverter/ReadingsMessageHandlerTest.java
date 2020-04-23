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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.concurrent.Future;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ReadingsMessageHandlerTest
{
    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveRequestThenRespondWithCorrectMetadata() throws Exception
    {
        final Producer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"C\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        ReadingsMessageHandler handler = new ReadingsMessageHandler(producer);
        handler.handleMessage(record);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());

        assertNull(sendArg.getValue().key());

        assertEquals("readings.responses", sendArg.getValue().topic());

        assertEquals(1, sendArg.getValue().headers().toArray().length);

        Header correlationId = sendArg.getValue().headers().lastHeader("$http.correlationId");
        assertNotNull("missing correlationId header", correlationId);
        assertArrayEquals("123".getBytes(UTF_8), correlationId.value());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveCelsiusRequestThenChangeUnitAndRespond() throws Exception
    {
        final Producer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"C\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        ReadingsMessageHandler handler = new ReadingsMessageHandler(producer);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.C, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        try (JsonReader outputJson = Json.createReader(new StringReader(sendArg.getValue().value())))
        {
            JsonObject output = outputJson.readObject();
            assertEquals(1, output.size());

            String unit = output.getString("unit");
            assertNotNull(unit);
            assertEquals("C", unit);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveFahrenheitRequestThenChangeUnitAndRespond() throws Exception
    {
        final Producer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"F\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        ReadingsMessageHandler handler = new ReadingsMessageHandler(producer);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.F, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        try (JsonReader outputJson = Json.createReader(new StringReader(sendArg.getValue().value())))
        {
            JsonObject output = outputJson.readObject();
            assertEquals(1, output.size());

            String unit = output.getString("unit");
            assertNotNull(unit);
            assertEquals("F", unit);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveKelvinRequestAndThenChangeUnitAndRespond() throws Exception
    {
        final Producer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"K\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        ReadingsMessageHandler handler = new ReadingsMessageHandler(producer);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.K, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        try (JsonReader outputJson = Json.createReader(new StringReader(sendArg.getValue().value())))
        {
            JsonObject output = outputJson.readObject();
            assertEquals(1, output.size());

            String unit = output.getString("unit");
            assertNotNull(unit);
            assertEquals("K", unit);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSkipRequestWithNonJsonPayload() throws Exception
    {
        final Producer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("xyz-not-JSON");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        ReadingsMessageHandler handler = new ReadingsMessageHandler(producer);
        handler.handleMessage(record);

        verify(producer, never()).send(any());

        // Ensure that a subsequent valid message is processed correctly
        recordHeaders = new RecordHeaders();
        recordHeaders.add("$http.replyTo", "readings.responses".getBytes(UTF_8));
        recordHeaders.add("$http.correlationId", "123".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.value()).thenReturn("{\"unit\":\"C\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        handler = new ReadingsMessageHandler(producer);
        handler.handleMessage(record);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());

        assertNull(sendArg.getValue().key());

        assertEquals("readings.responses", sendArg.getValue().topic());

        assertEquals(1, sendArg.getValue().headers().toArray().length);

        Header correlationId = sendArg.getValue().headers().lastHeader("$http.correlationId");
        assertNotNull("missing correlationId header", correlationId);
        assertArrayEquals("123".getBytes(UTF_8), correlationId.value());
    }

}
