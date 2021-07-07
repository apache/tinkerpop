package org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain;

import org.apache.tinkerpop.gremlin.language.examples.thrift.ValueEncoding;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class EncodingUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ValueEncoding createEncoding(String typeName, Class javaClass) {
        return new ValueEncoding() {
            @Override
            public String getTypeName() {
                return typeName;
            }

            @Override
            public Class getJavaClass() {
                return javaClass;
            }

            @Override
            public Object decode(String encoded) throws IOException {
                return objectMapper.readValue(encoded, javaClass);
            }

            @Override
            public String encode(Object decoded) throws IOException {
                return objectMapper.writeValueAsString(decoded);
            }
        };
    }
}
