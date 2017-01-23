package com.designnet.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.Properties;

/**
 * Created by nandhini on 15/1/17.
 */
@Configuration
@EnableKafka
public class KafkaStreamConfig {


    @Value("${applicationId}")
    String applicationId;

    @Value("${serverConfig}")
    String serverConfig;

    @Value("${offsetConfig}")
    String offsetConfig;

    @Value("${inputTopic}")
    String inputTopic;

    @Value(("${outputTopic}"))
    String outputTopic;

    @Value("${twitterApplicationId}")
    String twitterApplicationId;

    @Value("${twitterInputTopic}")
    String twitterInputTopic;

    @Value(("${twitterOutputTopic}"))
    String twitterOutputTopic;

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public String getTwitterInputTopic() {
        return twitterInputTopic;
    }

    public String getTwitterOutputTopic() {
        return twitterOutputTopic;
    }

    @Bean
    public Properties getKafkaStreamConfigs() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverConfig);
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);

        return properties;
    }

    public void wordCountTopology(){

        TopologyBuilder builder = new TopologyBuilder();

        StateStoreSupplier countStore = Stores.create("Counts")
                .withKeys(Serdes.String())
                .withValues(Serdes.Long())
                .persistent()
                .build();


        builder.addSource("Source", inputTopic)
                .addProcessor("Process", () -> new CountProcessor("Counts"), "Source")
                .addStateStore(countStore, "Process")
                .addSink("Sink", outputTopic, "Process");

        Properties kafkaStreamConfigs = getKafkaStreamConfigs();
        kafkaStreamConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

        KafkaStreams kafkaStreams = new KafkaStreams(builder, kafkaStreamConfigs);
        kafkaStreams.start();

//            Add shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }


    public void twitterWordCountTopology(){


        TopologyBuilder builder = new TopologyBuilder();

        StateStoreSupplier countStore = Stores.create("twitter-counts")
                .withKeys(Serdes.String())
                .withValues(Serdes.Long())
                .persistent()
                .build();


        builder.addSource("twitter-source", twitterInputTopic)
                .addProcessor("twitter-process", () -> new CountProcessor("twitter-counts"), "twitter-source")
                .addStateStore(countStore, "twitter-process")
                .addSink("twitter-sink", twitterOutputTopic, "twitter-process");

        Properties twitterKafkaStreamConfigs = getKafkaStreamConfigs();
        twitterKafkaStreamConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG, twitterApplicationId);

        KafkaStreams kafkaStreams = new KafkaStreams(builder, twitterKafkaStreamConfigs);
        kafkaStreams.start();

//            Add shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }


}
