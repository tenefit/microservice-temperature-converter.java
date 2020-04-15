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
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.NotBlank;
import com.github.rvesse.airline.annotations.restrictions.NotEmpty;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;

@Command(name = "temperature-converter", description = "Microservice for converting temperatures")
public class TemperatureConverter
{
    private final Duration kafkaPollTimeout = Duration.ofSeconds(1000);

    @Inject
    protected HelpOption<TemperatureConverter> help;

    @Option(
        name = { "--bootstrap-servers", "-b" },
        description = "Address for Kafka. e.g. kafka:9092")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String kafkaAddress;

    @Option(
        name = { "--sensors-topic" },
        description = "Input topic with raw sensor readings")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String sensorsTopic;

    @Option(
        name = { "--readings-topic" },
        description = "Output topic for converted readings")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String readingsTopic;

    @Option(
        name = { "--readings-requests-topic" },
        description = "Input topic for microservice command requests")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String readingsRequestsTopic;

    @Option(
        name = "--kafka-consumer-property",
        description = "Kafka consumer property. May be repeated. Format: <key>=<value>. e.g. session.timeout.ms=5000")
    @NotBlank
    @NotEmpty
    private List<String> kafkaConsumerPropertiesArgs;

    @Option(
        name = "--kafka-producer-property",
        description = "Kafka producer property. May be repeated. Format: <key>=<value>. e.g. batch.size=16384")
    @NotBlank
    @NotEmpty
    private List<String> kafkaProducerPropertiesArgs;

    private final Properties kafkaConsumerOptions;
    private final Properties kafkaProducerOptions;

    private KafkaConsumer<String, String> consumer;

    private boolean isRunning = true;

    private TemperatureUnit currentTempUnit;

    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        SingleCommand<TemperatureConverter> parser = SingleCommand.singleCommand(TemperatureConverter.class);
        TemperatureConverter microservice = parser.parse(args);
        microservice.start();
    }

    public TemperatureConverter() throws Exception
    {
        currentTempUnit = TemperatureUnit.F;

        kafkaConsumerOptions = new Properties();
        kafkaProducerOptions = new Properties();
    }

    public void start() throws InterruptedException, ExecutionException
    {
        processCommandLine();

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

        try
        {
            startListening();
        }
        catch (KafkaException e)
        {
            if (e instanceof InterruptException)
            {
                // User pressed Ctrl-C while Kafka library was blocking. Do nothing.
            }
            else
            {
                // Most likely an invalid Kafka address.
                System.out.format("ERROR: %s\n", e);
                if (e.getCause() != null)
                {
                    System.out.format("Cause: %s\n", e.getCause().getMessage());
                }
            }
            return;
        }

    }

    private void processCommandLine()
    {
        if (help.showHelpIfRequested())
        {
            return;
        }

        kafkaConsumerOptions.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        kafkaConsumerOptions.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerOptions.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        kafkaProducerOptions.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        kafkaProducerOptions.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerOptions.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (kafkaConsumerPropertiesArgs != null)
        {
            kafkaConsumerPropertiesArgs.forEach(this::parseKafkaConsumerProperty);
        }
        if (kafkaProducerPropertiesArgs != null)
        {
            kafkaProducerPropertiesArgs.forEach(this::parseKafkaProducerProperty);
        }
    }

    private void parseKafkaConsumerProperty(String arg)
    {
        String[] pair = arg.split("=");
        kafkaConsumerOptions.put(pair[0], pair[1]);
    }

    private void parseKafkaProducerProperty(String arg)
    {
        String[] pair = arg.split("=");
        kafkaProducerOptions.put(pair[0], pair[1]);
    }

    private void startListening() throws InterruptedException, ExecutionException
    {
        this.consumer = new KafkaConsumer<>(kafkaConsumerOptions);

        List<PartitionInfo> sensorsPartitions = consumer.partitionsFor(sensorsTopic);
        if (sensorsPartitions == null)
        {
            System.err.format("ERROR: The topic [%s] does not exist\n", sensorsTopic);
            return;
        }

        List<PartitionInfo> readingsRequestsPartitions = consumer.partitionsFor(readingsRequestsTopic);
        if (readingsRequestsPartitions == null)
        {
            System.err.format("ERROR: The topic [%s] does not exist\n", readingsRequestsTopic);
            return;
        }

        final Set<TopicPartition> topicPartitions = new HashSet<>();
        sensorsPartitions.forEach(p -> topicPartitions.add(new TopicPartition(p.topic(), p.partition())));
        readingsRequestsPartitions.forEach(p -> topicPartitions.add(new TopicPartition(p.topic(), p.partition())));

        consumer.assign(topicPartitions);

        SensorsMessageHandler sensorsMessageHandler = new SensorsMessageHandler(
            new KafkaProducer<String, String>(kafkaProducerOptions),
            readingsTopic
            );

        ReadingsMessageHandler readingsMessageHandler = new ReadingsMessageHandler(
            new KafkaProducer<String, String>(kafkaProducerOptions));

        System.out.println("TemperatureConverter microservice listening");

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
                if (record.topic().equals(sensorsTopic))
                {
                    sensorsMessageHandler.handleMessage(record, currentTempUnit);
                }
                else
                {
                    final TemperatureUnit newTempUnit = readingsMessageHandler.handleMessage(record);
                    if (newTempUnit != null)
                    {
                        currentTempUnit = newTempUnit;
                    }
                }
            }
        }
    }
}
