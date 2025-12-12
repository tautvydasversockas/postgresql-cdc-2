package custom.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

public class PostgresWalSmt<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value == null) {
            return record;
        }

        if (!(value instanceof Struct)) {
            throw new DataException("Record is expected to be 'Struct'.");
        }

        Struct rootStruct = (Struct) value;
        Struct messageStruct = rootStruct.getStruct("message");
        if (messageStruct == null) {
            throw new DataException("Missing 'message' section in record.");
        }

        String prefixString = messageStruct.getString("prefix");
        if (prefixString == null) {
            throw new DataException("Missing 'prefix' section in message.");
        }

        JsonNode prefixNode;
        try {
            prefixNode = mapper.readTree(prefixString);
        } catch (Exception e) {
            throw new DataException("Failed to parse prefix JSON", e);
        }

        String topic;
        if (prefixNode.hasNonNull("topic")) {
            topic = prefixNode.get("topic").asText();
        } else {
            throw new DataException("Missing 'topic' in prefix.");
        }

        String messageKey;
        if (prefixNode.hasNonNull("message_key")) {
            messageKey = prefixNode.get("message_key").asText();
        } else {
            throw new DataException("Missing 'message_key' in prefix.");
        }

        JsonNode headersNode = prefixNode.get("message_headers");
        if (headersNode == null || !headersNode.isObject()) {
            throw new DataException("Missing 'message_headers' in prefix.");
        }

        ConnectHeaders newHeaders = new ConnectHeaders();
        Iterator<Map.Entry<String, JsonNode>> fields = headersNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String headerKey = field.getKey();
            JsonNode headerValue = field.getValue();
            newHeaders.add(headerKey, headerValue.asText(), Schema.STRING_SCHEMA);
        }

        byte[] contentBytes = messageStruct.getBytes("content");
        if (contentBytes == null) {
            throw new DataException("Missing 'content' in message.");
        }

        String contentString = new String(contentBytes, StandardCharsets.UTF_8);

        return record.newRecord(
                topic,
                null,
                Schema.STRING_SCHEMA,
                messageKey,
                Schema.STRING_SCHEMA,
                contentString,
                record.timestamp(),
                newHeaders);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }
}
