# Kafka Streams example

This is an example project demonstrating the use of Kafka Streams using the low-level API.

## Word count demo

The example includes a WordCount demo. Read data to a kafka inputTopic, count the words and output the data to an outputTopic.


## Twitter word count demo

The example reads from a twitter user, clean the data by removing numbers and external links and count the number of words.

NOTE : Change the configuration in config/application.properties and run MainApplication.java