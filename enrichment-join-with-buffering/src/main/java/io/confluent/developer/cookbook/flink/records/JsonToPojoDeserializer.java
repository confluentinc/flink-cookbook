package io.confluent.developer.cookbook.flink.records;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class JsonToPojoDeserializer<T> extends AbstractDeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> clazz;

    private transient ObjectMapper objectMapper;

    public JsonToPojoDeserializer(Class<T> clazz) {
        super(clazz);
        this.clazz = clazz;
    }

    @Override
    public void open(InitializationContext context) {
        objectMapper = JsonMapper.builder().build();
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, clazz);
    }
}
