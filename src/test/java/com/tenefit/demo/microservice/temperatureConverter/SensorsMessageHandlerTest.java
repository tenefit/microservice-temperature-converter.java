/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

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
import com.tenefit.demo.microservice.temperatureConverter.TempUtils.TemperatureUnit;

public class SensorsMessageHandlerTest
{
    // TODO Is this the correct way to do a global member variable?
    private Gson gson = new Gson();

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveSensorMessageAndPublishReading() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        when(record.key()).thenReturn("1");
        when(record.value()).thenReturn("{\"id\":\"1\",\"unit\":\"F\",\"value\":32}");
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("row", "1".getBytes());
        when(record.headers()).thenReturn(recordHeaders);
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        SensorsMessageHandler handler = new SensorsMessageHandler("readings", kafkaProducerFactory, props);
        handler.handleMessage(record, TemperatureUnit.C);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        assertEquals("readings", sendArg.getValue().topic());
        assertEquals("1", sendArg.getValue().key());
        JsonObject message = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(3, message.keySet().size());
        JsonElement idEl = message.get("id");
        assertNotNull(idEl);
        assertEquals("1", idEl.getAsString());
        JsonElement unitEl = message.get("unit");
        assertNotNull(unitEl);
        assertEquals("C", unitEl.getAsString());
        JsonElement valueEl = message.get("value");
        assertNotNull(valueEl);
        assertEquals(0, valueEl.getAsInt());
        Header[] headers = sendArg.getValue().headers().toArray();
        assertEquals(1, headers.length);
        Optional<Header> rowHeader = Arrays.stream(headers).filter(h -> h.key().equals("row")).findFirst();
        assertTrue("no row header", rowHeader.isPresent());
        String row = new String(rowHeader.get().value());
        assertEquals("1", row);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveCelsiusRequestAndRespond() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        when(record.value()).thenReturn("{\"unit\":\"C\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        ReadingsMessageHandler handler = new ReadingsMessageHandler("readings.responses", kafkaProducerFactory, props);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.C, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        assertEquals("readings.responses", sendArg.getValue().topic());
        assertNull(sendArg.getValue().key());
        JsonObject message = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(1, message.keySet().size());
        JsonElement unitEl = message.get("unit");
        assertNotNull(unitEl);
        assertEquals("C", unitEl.getAsString());
        Header[] headers = sendArg.getValue().headers().toArray();
        assertEquals(0, headers.length);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveFahrenheitRequestAndRespond() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        when(record.value()).thenReturn("{\"unit\":\"F\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        ReadingsMessageHandler handler = new ReadingsMessageHandler("readings.responses", kafkaProducerFactory, props);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.F, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        assertEquals("readings.responses", sendArg.getValue().topic());
        assertNull(sendArg.getValue().key());
        JsonObject message = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(1, message.keySet().size());
        JsonElement unitEl = message.get("unit");
        assertNotNull(unitEl);
        assertEquals("F", unitEl.getAsString());
        Header[] headers = sendArg.getValue().headers().toArray();
        assertEquals(0, headers.length);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveKelvinRequestAndRespond() throws Exception
    {
        final KafkaProducerFactory kafkaProducerFactory = mock(KafkaProducerFactory.class);
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        when(kafkaProducerFactory.newKafkaProducer(any(Properties.class))).thenReturn(producer);
        when(record.value()).thenReturn("{\"unit\":\"K\"}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        Properties props = new Properties();
        ReadingsMessageHandler handler = new ReadingsMessageHandler("readings.responses", kafkaProducerFactory, props);
        TemperatureUnit newTempUnit = handler.handleMessage(record);

        assertEquals(TemperatureUnit.K, newTempUnit);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());
        assertEquals("readings.responses", sendArg.getValue().topic());
        assertNull(sendArg.getValue().key());
        JsonObject message = gson.fromJson(sendArg.getValue().value(), JsonObject.class);
        assertEquals(1, message.keySet().size());
        JsonElement unitEl = message.get("unit");
        assertNotNull(unitEl);
        assertEquals("K", unitEl.getAsString());
        Header[] headers = sendArg.getValue().headers().toArray();
        assertEquals(0, headers.length);
    }
}
