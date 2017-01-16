package com.designnet;

import com.designnet.producer.MessageProducer;
import com.designnet.stream.CountProcessor;
import com.designnet.stream.KafkaStreamConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            LOGGER.info("Inside main");

            KafkaStreamConfig kafkaConfiguration = ctx.getBean(KafkaStreamConfig.class);

//            MessageProducer messageProducer = ctx.getBean(MessageProducer.class);
//            messageProducer.sendMessage(kafkaConfiguration.getInputTopic(), "Spring");

            TopologyBuilder builder = new TopologyBuilder();

            StateStoreSupplier countStore = Stores.create("Counts")
                    .withKeys(Serdes.String())
                    .withValues(Serdes.Long())
                    .persistent()
                    .build();


            builder.addSource("Source", kafkaConfiguration.getInputTopic())
                    .addProcessor("Process", () -> new CountProcessor(), "Source")
                    .addStateStore(countStore, "Process")
                    .addSink("Sink", kafkaConfiguration.getOutputTopic(), "Process");

            KafkaStreams kafkaStreams = new KafkaStreams(builder, kafkaConfiguration.getKafkaStreamConfigs());
            kafkaStreams.start();

//            Add shutdown hook

            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


        };
    }
}
