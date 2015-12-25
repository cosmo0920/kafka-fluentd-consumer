package org.fluentd.kafka.parser;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonAutoDetect;

import org.fluentd.kafka.PropertyConfig;

public class JsonParser extends MessageParser {
    private final static ObjectMapper mapper = new ObjectMapper(new JsonFactory());
    private final static TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

    public JsonParser(PropertyConfig config) {
        super(config);
    }

    @Override
    public Map<String, Object> parse(MessageAndMetadata<byte[], byte[]> entry) throws Exception {
        try {
            return mapper.readValue(new String(entry.message()), typeRef);
        } catch (IOException e) {
            throw new RuntimeException(e); // Avoid IOException conflict with fluency logger
        }
    }

    @Override
    public Map<String, Object> parse(ConsumerRecord<String, String> record) throws Exception {
        try {
            return mapper.readValue(record.value(), typeRef);
        } catch (IOException e) {
            throw new RuntimeException(e); // Avoid IOException conflict with fluency logger
        }
    }
}
