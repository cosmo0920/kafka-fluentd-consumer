package org.fluentd.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.komamitsu.fluency.Fluency;
import org.fluentd.kafka.parser.MessageParser;
import org.fluentd.kafka.parser.JsonParser;
import org.fluentd.kafka.parser.RegexpParser;

public class FluentdHandler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FluentdHandler.class);

    private final PropertyConfig config;
    private final FluentdTagger tagger;
    private final KafkaStream stream;
    private final Fluency logger;
    private final MessageParser parser;
    private final String timeField;
    private final SimpleDateFormat formatter;
    private final ConsumerRecords records;

    public FluentdHandler(KafkaStream stream, PropertyConfig config, Fluency logger) {
        this.config = config;
        this.tagger = config.getTagger();
        this.stream = stream;
        this.records = null;
        this.logger = logger;
        this.parser = setupParser();
        this.timeField = config.get("fluentd.record.time.field", null);
        this.formatter = setupTimeFormatter();
    }

    public FluentdHandler(ConsumerRecords records, PropertyConfig config, Fluency logger) {
        this.config = config;
        this.tagger = config.getTagger();
        this.stream = null;
        this.records = records;
        this.logger = logger;
        this.parser = setupParser();
        this.timeField = config.get("fluentd.record.time.field", null);
        this.formatter = setupTimeFormatter();
    }

    public void newConsumerRun() {
        Iterator<ConsumerRecord<String, String>> it = records.iterator();
        while (it.hasNext()) {
            ConsumerRecord<String, String> record = it.next();

            try {
                try {
                    Map<String, Object> data = parser.parse(record);
                    // TODO: Add kafka metadata like metada and topic
                    // TODO: Improve performance with batch insert and need to fallback feature to another fluentd instance
                    if (timeField == null) {
                        logger.emit(tagger.generate(record.topic()), data);
                    } else {
                        long time;
                        try {
                            time = formatter.parse((String)data.get(timeField)).getTime() / 1000;
                        } catch (Exception e) {
                            LOG.warn("failed to parse event time: " + e.getMessage());
                            time = System.currentTimeMillis() / 1000;
                        }
                        logger.emit(tagger.generate(record.topic()), time, data);
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    Map<String, Object> data = new HashMap<String, Object>();
                    data.put("message", record.value());
                    logger.emit("failed", data); // should be configurable
                }
            } catch (IOException e) {
                LOG.error("can't send a log to fluentd. Wait 1 second", e);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ie) {
                    LOG.warn("Interrupted during sleep");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> entry = it.next();

            try {
                try {
                    Map<String, Object> data = parser.parse(entry);
                    // TODO: Add kafka metadata like metada and topic
                    // TODO: Improve performance with batch insert and need to fallback feature to another fluentd instance
                    if (timeField == null) {
                        logger.emit(tagger.generate(entry.topic()), data);
                    } else {
                        long time;
                        try {
                            time = formatter.parse((String)data.get(timeField)).getTime() / 1000;
                        } catch (Exception e) {
                            LOG.warn("failed to parse event time: " + e.getMessage());
                            time = System.currentTimeMillis() / 1000;
                        }
                        logger.emit(tagger.generate(entry.topic()), time, data);
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    Map<String, Object> data = new HashMap<String, Object>();
                    data.put("message", new String(entry.message()));
                    logger.emit("failed", data); // should be configurable
                }
            } catch (IOException e) {
                LOG.error("can't send a log to fluentd. Wait 1 second", e);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ie) {
                    LOG.warn("Interrupted during sleep");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private MessageParser setupParser()
    {
        String format = config.get("fluentd.record.format", "json");
        switch (format) {
        case "json":
            return new JsonParser(config);
        case "regexp":
            return new RegexpParser(config);
        default:
            throw new RuntimeException(format + " format is not supported");
        }
    }

    private SimpleDateFormat setupTimeFormatter() {
        if (timeField == null)
            return null;

        return new SimpleDateFormat(config.get("fluentd.record.time.pattern"));
    }
}
