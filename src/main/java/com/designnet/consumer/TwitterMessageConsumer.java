package com.designnet.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

/**
 * Created by nandhini on 13/1/17.
 */
public class TwitterMessageConsumer {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(TwitterMessageConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);



    @KafkaListener(id = "test1", topics = "wordCount-twitterOutputTopic", group = "testGroup8")
    public void receiveMessage(String message) {
        LOGGER.info("received message='{}'", message);
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
