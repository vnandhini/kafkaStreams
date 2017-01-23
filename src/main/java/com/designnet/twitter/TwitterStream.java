package com.designnet.twitter;

import com.designnet.producer.MessageProducer;
import com.designnet.stream.KafkaStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Controller;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;

/**
 * Created by nandhini on 17/1/17.
 */
@Controller
public class TwitterStream {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(TwitterStream.class);

    @Autowired
    MessageProducer messageProducer;

    @Value("${consumer_key}")
    String consumerKey;

    @Value("${consumer_secret}")
    String consumerSecret;

    @Value("${access_token}")
    String accessToken;

    @Value("${access_token_secret}")
    String accessTokenSecret;

    @Autowired
    KafkaStreamConfig kafkaStreamConfig;


    public MessageProducer getMessageProducer() {
        return messageProducer;
    }

    public void listenTwitter(){

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);

        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        List<Status> statuses = null;
        try {
            statuses = twitter.getUserTimeline("Linkedin");
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        LOGGER.info("Showing home timeline.");
        for (Status status : statuses) {

            //Clean text
            String tweet = status.getText();
            tweet = tweet.replaceAll("http.*?\\s", "");
            tweet = tweet.replaceAll("[^a-zA-Z ]", "").toLowerCase();

            LOGGER.info(tweet);

            messageProducer.sendMessage(kafkaStreamConfig.getTwitterInputTopic(), tweet);



        }

    }

}
