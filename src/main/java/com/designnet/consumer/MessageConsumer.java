package com.designnet.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

/**
 * Created by nandhini on 13/1/17.
 */
public class MessageConsumer {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MessageConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);



    @KafkaListener(id = "test1", topics = "outputTopic8", group = "testGroup")
    public void receiveMessage(String message) {
        LOGGER.info("received message='{}'", message);
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
