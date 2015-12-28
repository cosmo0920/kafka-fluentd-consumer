package org.fluentd.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerTask implements Callable<ConsumerRecords<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTask.class);

    private final String topicProp;
    private final PropertyConfig config;
    private final KafkaConsumer consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaConsumerTask(PropertyConfig config) throws IOException {
        this.config = config;
        this.consumer = new KafkaConsumer<String, String>(config.getProperties());
        this.topicProp = config.get(PropertyConfig.Constants.FLUENTD_CONSUMER_TOPICS.key);
    }

    public void shutdown() {
        LOG.info("Shutting down consumers");

        if (consumer != null) {
            closed.set(true);
            consumer.wakeup();
            consumer.close();
        }
    }

    @Override
    public ConsumerRecords<String, String> call() {
        ConsumerRecords<String, String> records = null;
        try {
            setupKafkaConsumer();
            records = consumer.poll(10000);
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            // Do Nothing!
        }

        return records;
    }

    public void setupKafkaConsumer() {
        String topics = config.get(PropertyConfig.Constants.FLUENTD_CONSUMER_TOPICS.key);
        Pattern pattern = Pattern.compile(topics);
        consumer.subscribe(pattern, new NoOpConsumerListener(consumer));
    }

    private class NoOpConsumerListener implements ConsumerRebalanceListener {
        private KafkaConsumer<?,?> consumer;

        public NoOpConsumerListener(KafkaConsumer<?,?> consumer) {
            this.consumer = consumer;
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }
    }
}
