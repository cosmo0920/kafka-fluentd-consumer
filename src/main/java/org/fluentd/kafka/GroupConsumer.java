package org.fluentd.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;

import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class GroupConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(GroupConsumer.class);

    private final String topicProp;
    private final PropertyConfig config;
    private ExecutorService executor;
    private final Fluency fluentLogger;
    private final KafkaConsumerTask consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public GroupConsumer(PropertyConfig config) throws IOException {
        this.config = config;
        this.consumer = new KafkaConsumerTask(config);
        this.topicProp = config.get(PropertyConfig.Constants.FLUENTD_CONSUMER_TOPICS.key);
        this.fluentLogger = setupFluentdLogger();

        // for testing. Don't use on production
        if (config.getBoolean(PropertyConfig.Constants.FLUENTD_CONSUMER_FROM_BEGINNING.key, false))
            ZkUtils.maybeDeletePath(config.get(PropertyConfig.Constants.KAFKA_ZOOKEEPER_CONNECT.key), "/consumers/" + config.get(PropertyConfig.Constants.KAFKA_GROUP_ID.key));
    }

    public Fluency setupFluentdLogger() throws IOException {
        return Fluency.defaultFluency(config.getFluentdConnect());
    }

    public void shutdown() {
        LOG.info("Shutting down consumers");

        if (consumer != null) {
            closed.set(true);
            consumer.shutdown();
        }
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted during shutdown, exiting uncleanly");
            executor.shutdownNow();
        }

        try {
            fluentLogger.close();
        } catch (IOException e) {
            LOG.error("failed to close fluentd logger completely", e);
        }
    }

    public void run() {
        try {
            int numThreads = config.getInt(PropertyConfig.Constants.FLUENTD_CONSUMER_THREADS.key);
            executor = Executors.newFixedThreadPool(numThreads);
            while (!closed.get()) {
                try {
                    Future<ConsumerRecords<String, String>> future = executor.submit(consumer);
                    ConsumerRecords<String, String> records = future.get();
                    executor.submit(new FluentdHandler(records, config, fluentLogger));
                } catch (InterruptedException e) {
                    LOG.error("Interrupted during consuming");
                } catch (ExecutionException e){
                    LOG.error("Got exception during executing " + e.getCause());
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            consumer.shutdown();
        }
    }

    public static void main(String[] args) throws IOException {
        final PropertyConfig pc = new PropertyConfig(args[0]);
        final GroupConsumer gc = new GroupConsumer(pc);

        gc.run();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    gc.shutdown();
                }
            }));

        try {
            // Need better long running approach.
            while (true) {
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            LOG.error("Something happen!", e);
        }
    }
}
