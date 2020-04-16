/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
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
    private final String defaultGroupId = "temperature-converter";

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
        name = { "--input-topic" },
        description = "Input topic with raw sensor readings")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String inputTopic;

    @Option(
        name = { "--output-topic" },
        description = "Output topic for converted readings")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String outputTopic;

    @Option(
        name = { "--requests-topic" },
        description = "Input topic for microservice command requests")
    @Required
    @Once
    @NotBlank
    @NotEmpty
    private String requestsTopic;

    @Option(
        name = "--consumer-property",
        description = "Kafka consumer property. May be repeated. Format: <key>=<value>. e.g. session.timeout.ms=5000")
    @NotBlank
    @NotEmpty
    private List<String> consumerPropertiesArgs;

    @Option(
        name = "--producer-property",
        description = "Kafka producer property. May be repeated. Format: <key>=<value>. e.g. batch.size=16384")
    @NotBlank
    @NotEmpty
    private List<String> producerPropertiesArgs;

    private final Properties consumerOptions;
    private final Properties producerOptions;

    private volatile boolean isRunning = true;

    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        SingleCommand<TemperatureConverter> parser = SingleCommand.singleCommand(TemperatureConverter.class);
        TemperatureConverter microservice = parser.parse(args);
        microservice.start();
    }

    public TemperatureConverter() throws Exception
    {
        consumerOptions = new Properties();
        consumerOptions.put("group.id", defaultGroupId);

        producerOptions = new Properties();
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
                catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }
        });

        try
        {
            startListening();
        }
        catch (InterruptException ex)
        {
            // User pressed Ctrl-C while Kafka library was blocking. Do nothing.
        }
        catch (KafkaException ex)
        {
            // Most likely an invalid Kafka address.
            System.out.format("ERROR: %s\n", ex);
            if (ex.getCause() != null)
            {
                System.out.format("Cause: %s\n", ex.getCause().getMessage());
            }
        }

    }

    private void processCommandLine()
    {
        if (help.showHelpIfRequested())
        {
            return;
        }

        consumerOptions.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        consumerOptions.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerOptions.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        producerOptions.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        producerOptions.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerOptions.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (consumerPropertiesArgs != null)
        {
            consumerPropertiesArgs.forEach(this::parseKafkaConsumerProperty);
        }
        if (producerPropertiesArgs != null)
        {
            producerPropertiesArgs.forEach(this::parseKafkaProducerProperty);
        }
    }

    private void parseKafkaConsumerProperty(
        String arg)
    {
        String[] pair = arg.split("=");
        consumerOptions.put(pair[0], pair[1]);
    }

    private void parseKafkaProducerProperty(
        String arg)
    {
        String[] pair = arg.split("=");
        producerOptions.put(pair[0], pair[1]);
    }

    private void startListening() throws InterruptedException, ExecutionException
    {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerOptions))
        {
            consumer.subscribe(Arrays.asList(inputTopic, requestsTopic));

            SensorsMessageHandler sensorsMessageHandler = new SensorsMessageHandler(
                new KafkaProducer<String, String>(producerOptions),
                outputTopic);

            ReadingsMessageHandler readingsMessageHandler = new ReadingsMessageHandler(
                new KafkaProducer<String, String>(producerOptions));

            TemperatureUnit readingsUnit = TemperatureUnit.F;

            System.out.println("TemperatureConverter microservice listening");

            while (isRunning)
            {
                ConsumerRecords<String, String> records = consumer.poll(kafkaPollTimeout);
                for (ConsumerRecord<String, String> record: records)
                {
                    if (record.topic().equals(inputTopic))
                    {
                        sensorsMessageHandler.handleMessage(record, readingsUnit);
                    }
                    else
                    {
                        final TemperatureUnit newReadingsUnit = readingsMessageHandler.handleMessage(record);
                        if (newReadingsUnit != null)
                        {
                            readingsUnit = newReadingsUnit;
                        }
                    }
                }
            }
        }
    }
}
