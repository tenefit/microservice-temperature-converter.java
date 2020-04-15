/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
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

public class SensorsMessageHandlerTest
{
    private Gson gson = new Gson();

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReceiveSensorMessageThenPublishReading() throws Exception
    {
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        final ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        final Future<RecordMetadata> future = mock(Future.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("row", "1".getBytes(UTF_8));
        when(record.headers()).thenReturn(recordHeaders);
        when(record.key()).thenReturn("1");
        when(record.value()).thenReturn("{\"id\":\"1\",\"unit\":\"C\",\"value\":0}");
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        SensorsMessageHandler handler = new SensorsMessageHandler(producer, "readings");
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
}
