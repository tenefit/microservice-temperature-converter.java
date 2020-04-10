/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class SensorsConsumer implements AutoCloseable, Runnable
{
    private final Duration kafkaPollTimeout = Duration.ofSeconds(1000);

    private final Properties consumerProps;
    private final Properties producerProps;

    private final String sensorsTopic;
    private final String readingsTopic;

    private KafkaConsumer<String, String> consumer;

    private static boolean isRunning = true;

    public SensorsConsumer(
        String sensorsTopic,
        String readingsTopic,
        final Properties consumerProps,
        final Properties producerProps)
    {
        this.consumerProps = consumerProps;
        this.producerProps = producerProps;
        this.sensorsTopic = sensorsTopic;
        this.readingsTopic = readingsTopic;

        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    public void close()
    {
        consumer.close();
    }

    @Override
    public void run()
    {
        System.out.println("SensorsConsumer reading");
        final Thread myThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                isRunning = false;
                myThread.interrupt();
                try
                {
                    myThread.join();
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        });

        // this.consumer = new KafkaConsumer<>(consumeProps);

        List<PartitionInfo> partitions = consumer.partitionsFor(sensorsTopic);
        if (partitions == null)
        {
            System.err.format("Warning: The topic [%s] does not exist\n", sensorsTopic);
            return;
        }
        System.out.format("partitions=%s", partitions);
        final Set<TopicPartition> topicPartitions = new HashSet<>();
        partitions.forEach(p -> topicPartitions.add(new TopicPartition(p.topic(), p.partition())));

        consumer.assign(topicPartitions);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        while (isRunning)
        {
            ConsumerRecords<String, String> records = consumer.poll(kafkaPollTimeout);

            if (records.isEmpty())
            {
                continue;
            }

            Iterator<ConsumerRecord<String, String>> recordsIterator = records.iterator();
            while (recordsIterator.hasNext())
            {
                ConsumerRecord<String, String> record = recordsIterator.next();
                System.out.format("[%s] %s\n", record.key(), record.value());


            }
        }

        System.out.format("all done, %b\n", isRunning);
    }

}
