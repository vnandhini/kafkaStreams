package com.designnet.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
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

    public String getInputTopic() {
        return inputTopic;
    }

    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    @Bean
    public Properties getKafkaStreamConfigs() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverConfig);
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);

        return properties;
    }


}
