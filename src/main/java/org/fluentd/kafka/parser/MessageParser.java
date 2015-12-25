package org.fluentd.kafka.parser;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.fluentd.kafka.PropertyConfig;

public abstract class MessageParser {
    protected PropertyConfig config;

    public MessageParser(PropertyConfig config) {
        this.config = config;
    }

    public abstract Map<String, Object> parse(ConsumerRecord<String, String> entry) throws Exception;
}
