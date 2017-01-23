package com.designnet;

import com.designnet.consumer.MessageConsumer;
import com.designnet.producer.MessageProducer;
import com.designnet.stream.KafkaStreamConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

/**
 * Created by nandhini on 16/1/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MainApplicationTest {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MainApplicationTest.class);

    @Autowired
    private MessageProducer messageProducer;

    @Autowired
    private KafkaStreamConfig kafkaStreamConfig;

    @Autowired
    private MessageConsumer messageConsumer;

    private Map<String, Object> getConsumerConfig() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup2");
        return props;
    }

    @Test
    public void testApplication()throws Exception{

        //Message to send
        final String messageToSend = "stream";

        // Create the expected word counts
        List<KeyValue<String, Long>> expectedWordCounts = new ArrayList<>();
        expectedWordCounts.add(new KeyValue<>("stream",2L));
        expectedWordCounts.add(new KeyValue<>("stream",3L));
        expectedWordCounts.add(new KeyValue<>("stream",4L));


        // Actual word count
        List<KeyValue<String, Long>> actualWordCounts = new ArrayList<>();

        //Send message
        messageProducer.sendMessage(kafkaStreamConfig.getInputTopic(), messageToSend);

        // Create a KafkaConsumer for the sink topic to check the received message

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(getConsumerConfig());
        consumer.subscribe(Collections.singletonList(kafkaStreamConfig.getOutputTopic()));
        messageConsumer.getLatch().await(10, TimeUnit.SECONDS);
        ConsumerRecords<String, Long> records = consumer.poll(10000);

        LOGGER.info("Has consumer consumed messages " + records.isEmpty());

        for (ConsumerRecord<String, Long> record : records) {
            actualWordCounts.add(new KeyValue<>(record.key(), record.value()));

//            LOGGER.info("Message consumed from sink : {}, Key : {}, Value : {}"
//                    , kafkaStreamConfig.getOutputTopic()
//                    , record.key()
//                    , String.valueOf(record.value()));

        }

        LOGGER.info("KTable from sink {} " , actualWordCounts);

        assertEquals(expectedWordCounts.toString(), actualWordCounts.toString());

    }

}
