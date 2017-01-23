package com.designnet.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

/**
 * Created by nandhini on 15/1/17.
 */
@Controller
public class CountProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(CountProcessor.class);

    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    private String stateStoreConfig = new String();

    public CountProcessor(){
        super();
    }

    public CountProcessor(String stateStoreConfig){
        super();
        this.stateStoreConfig = stateStoreConfig;

    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(1000);
//        this.kvStore = (KeyValueStore) processorContext.getStateStore("Counts");
        this.kvStore = (KeyValueStore) processorContext.getStateStore(stateStoreConfig);


    }

    @Override
    public void process(Object o, Object o2) {
        String line = String.valueOf(o2);
        String[] words = line.toLowerCase().split(" ");

        for (String word : words) {
            Long oldValue = kvStore.get(word);
            if (oldValue == null) {
                kvStore.put(word, 1L);
            } else {
                kvStore.put(word, oldValue + 1L);
            }
            LOGGER.info("Word: {}, Value : {}", word ,kvStore.get(word));
        }
    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator<String, Long> iter = this.kvStore.all();
        while (iter.hasNext()) {
            KeyValue<String, Long> entry = iter.next();
            context.forward(entry.key, entry.value.toString());
        }
        iter.close();
        // commit the current processing progress
        context.commit();

    }

    @Override
    public void close() {

        // close the key-value store
        kvStore.close();

    }
}
