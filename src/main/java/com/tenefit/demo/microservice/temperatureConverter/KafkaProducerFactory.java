package com.tenefit.demo.microservice.temperatureConverter;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

public interface KafkaProducerFactory
{
    default KafkaProducer<String, String> newKafkaProducer(
        Properties properties)
    {
        return new KafkaProducer<String, String>(properties);
    }
}
