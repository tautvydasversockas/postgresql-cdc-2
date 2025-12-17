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
import java.util.Map;

public class PostgresWalSmt<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public R apply(R record) {
        var value = record.value();
        if (!(value instanceof Struct rootStruct)) {
            if (value == null) {
                return record;
            }

            throw new DataException("Record is expected to be 'Struct'.");
        }

        var messageStruct = rootStruct.getStruct("message");
        if (messageStruct == null) {
            throw new DataException("Missing 'message' section in record.");
        }

        var prefixString = messageStruct.getString("prefix");
        if (prefixString == null) {
            throw new DataException("Missing 'prefix' section in message.");
        }

        JsonNode prefixNode;
        try {
            prefixNode = mapper.readTree(prefixString);
        } catch (Exception e) {
            throw new DataException("Failed to parse prefix JSON", e);
        }

        var topicNode = prefixNode.get("topic");
        if (topicNode == null || topicNode.isNull()) {
            throw new DataException("Missing 'topic' in prefix.");
        }
        var topic = topicNode.asText();

        var messageKeyNode = prefixNode.get("message_key");
        if (messageKeyNode == null || messageKeyNode.isNull()) {
            throw new DataException("Missing 'message_key' in prefix.");
        }
        var messageKey = messageKeyNode.asText();

        var headersNode = prefixNode.get("message_headers");
        if (headersNode == null || !headersNode.isObject()) {
            throw new DataException("Missing 'message_headers' in prefix.");
        }

        var newHeaders = new ConnectHeaders();
        var fields = headersNode.fields();
        while (fields.hasNext()) {
            var field = fields.next();
            newHeaders.add(field.getKey(), field.getValue().asText(), Schema.STRING_SCHEMA);
        }

        var contentBytes = messageStruct.getBytes("content");
        if (contentBytes == null) {
            throw new DataException("Missing 'content' in message.");
        }

        return record.newRecord(
                topic,
                null,
                Schema.STRING_SCHEMA,
                messageKey,
                Schema.STRING_SCHEMA,
                new String(contentBytes, StandardCharsets.UTF_8),
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
