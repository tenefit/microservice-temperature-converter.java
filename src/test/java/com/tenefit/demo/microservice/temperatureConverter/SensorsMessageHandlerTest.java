/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SensorsMessageHandlerTest
{
    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveSensorMessageThenPublishReading() throws Exception
    {
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("row", "9".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.key()).thenReturn("4");
        when(record.value()).thenReturn("{\"id\":\"4\",\"unit\":\"C\",\"value\":0}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        SensorsMessageHandler handler = new SensorsMessageHandler(producer, "readings");
        handler.handleMessage(record, TemperatureUnit.F);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());

        assertEquals("4", sendArg.getValue().key());

        assertEquals("readings", sendArg.getValue().topic());

        assertEquals(1, sendArg.getValue().headers().toArray().length);

        Header row = sendArg.getValue().headers().lastHeader("row");
        assertNotNull("missing row header", row);
        assertArrayEquals("9".getBytes(UTF_8), row.value());

        try (JsonReader outputJson = Json.createReader(new StringReader(sendArg.getValue().value())))
        {
            JsonObject output = outputJson.readObject();
            assertEquals(3, output.size());

            String id = output.getString("id");
            assertNotNull(id);
            assertEquals("4", id);

            String unit = output.getString("unit");
            assertNotNull(unit);
            assertEquals("F", unit);

            int value = output.getInt("value", Integer.MAX_VALUE);
            assertEquals(32, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSkipMessageWithNonJsonPayload() throws Exception
    {
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("row", "9".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.key()).thenReturn("4");
        when(record.value()).thenReturn("xyz-not-JSON");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        SensorsMessageHandler handler = new SensorsMessageHandler(producer, "readings");
        handler.handleMessage(record, TemperatureUnit.F);

        verify(producer, never()).send(any());

        // Ensure that a subsequent valid message is processed correctly
        recordHeaders = new RecordHeaders();
        recordHeaders.add("row", "9".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.key()).thenReturn("4");
        when(record.value()).thenReturn("{\"id\":\"4\",\"unit\":\"C\",\"value\":0}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        handler = new SensorsMessageHandler(producer, "readings");
        handler.handleMessage(record, TemperatureUnit.F);

        ArgumentCaptor<ProducerRecord<String, String>> sendArg = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(sendArg.capture());

        assertEquals("4", sendArg.getValue().key());

        assertEquals("readings", sendArg.getValue().topic());

        assertEquals(1, sendArg.getValue().headers().toArray().length);

        Header row = sendArg.getValue().headers().lastHeader("row");
        assertNotNull("missing row header", row);
        assertArrayEquals("9".getBytes(UTF_8), row.value());

        try (JsonReader outputJson = Json.createReader(new StringReader(sendArg.getValue().value())))
        {
            JsonObject output = outputJson.readObject();
            assertEquals(3, output.size());

            String id = output.getString("id");
            assertNotNull(id);
            assertEquals("4", id);

            String unit = output.getString("unit");
            assertNotNull(unit);
            assertEquals("F", unit);

            int value = output.getInt("value", Integer.MAX_VALUE);
            assertEquals(32, value);
        }
    }
}
