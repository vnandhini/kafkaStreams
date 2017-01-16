package com.designnet.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Created by nandhini on 13/1/17.
 */
public class MessageProducer {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MessageProducer.class);

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    public void sendMessage(String topic, String message){

        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate
                .send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            public void onFailure(Throwable throwable) {
                LOGGER.error("Message send failed: {}", throwable);
            }

            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
                LOGGER.info("Message sent successfully with offset {}", integerStringSendResult.getRecordMetadata().offset());

            }
        });

    }
}
