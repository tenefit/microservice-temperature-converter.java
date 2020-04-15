/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

public class SensorsMessageHandlerTest
{
    private Gson gson = new Gson();

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveSensorMessageThenPublishReading() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("row", "1".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.key()).thenReturn("1");
        when(record.value()).thenReturn("{\"id\":\"1\",\"unit\":\"C\",\"value\":0}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        SensorsMessageHandler handler = new SensorsMessageHandler("readings", kafkaProducerFactory, props);
        handler.handleMessage(record, TemperatureUnit.F);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        assertEquals("readings", sendArg.getValue().topic());
        assertEquals("1", sendArg.getValue().key());
        JsonObject messageAsJson = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(3, messageAsJson.keySet().size());
        JsonElement idAsJson = messageAsJson.get("id");
        assertNotNull(idAsJson);
        assertEquals("1", idAsJson.getAsString());
        JsonElement unitAsJson = messageAsJson.get("unit");
        assertNotNull(unitAsJson);
        assertEquals("F", unitAsJson.getAsString());
        JsonElement valueAsJson = messageAsJson.get("value");
        assertNotNull(valueAsJson);
        assertEquals(32, valueAsJson.getAsInt());
        Header[] headers = sendArg.getValue().headers().toArray();
        assertEquals(1, headers.length);
        Optional<Header> rowHeader = Arrays.stream(headers).filter(h -> h.key().equals("row")).findFirst();
        assertTrue("missing row header", rowHeader.isPresent());
        String row = new String(rowHeader.get().value());
        assertEquals("1", row);
    }

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

    @Test
    public void shouldConvertCelsiusToCelsius() throws Exception
    {
        assertEquals(0, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.C, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertCelsiusToFahrenheit() throws Exception
    {
        assertEquals(32, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.C, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertCelsiusToKelvin() throws Exception
    {
        assertEquals(273, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.C, TemperatureUnit.K));
    }

    @Test
    public void shouldConvertFahrenheitToCelsius() throws Exception
    {
        assertEquals(-18, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.F, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertFahrenheitToFahrenheit() throws Exception
    {
        assertEquals(0, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.F, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertFahrenheitToKelvin() throws Exception
    {
        assertEquals(255, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.F, TemperatureUnit.K));
    }

    @Test
    public void shouldConvertKelvinToCelsius() throws Exception
    {
        assertEquals(-273, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.K, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertKelvinToFahrenheit() throws Exception
    {
        assertEquals(-460, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.K, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertKelvinToKelvin() throws Exception
    {
        assertEquals(0, SensorsMessageHandler.convertTemperature(0, TemperatureUnit.K, TemperatureUnit.K));
    }
}
