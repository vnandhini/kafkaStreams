package com.designnet;

import com.designnet.producer.MessageProducer;
import com.designnet.stream.CountProcessor;
import com.designnet.stream.KafkaStreamConfig;
import com.designnet.twitter.TwitterStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * Created by nandhini on 13/1/17.
 */
@SpringBootApplication

public class MainApplication {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MainApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(MainApplication.class, args);
    }

    @Autowired
    KafkaStreamConfig kafkaStreamConfig;

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

//            MessageProducer messageProducer = ctx.getBean(MessageProducer.class);
//            messageProducer.sendMessage(kafkaStreamConfig.getInputTopic(), "Spring");

            kafkaStreamConfig.wordCountTopology();

//            TwitterStream twitterStream = ctx.getBean(TwitterStream.class);
//            twitterStream.listenTwitter();
//            kafkaStreamConfig.twitterWordCountTopology();


        };
    }
}
