package org.apache.tinkerpop.gremlin.language.examples.thrift;

import org.apache.tinkerpop.gremlin.language.property_graphs.SerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ValueEncodingRegistry {
    private final Map<Class, ValueEncoding> encodingByClass;
    private final Map<String, ValueEncoding> encodingByTypeName;

    public ValueEncodingRegistry(ValueEncoding... encodings) {
        encodingByClass = new HashMap<>();
        encodingByTypeName = new HashMap<>();

        for (ValueEncoding encoding : encodings) {
            encodingByClass.put(encoding.getJavaClass(), encoding);
            encodingByTypeName.put(encoding.getTypeName(), encoding);
        }
    }

    public Object decode(SerializedValue encoded) throws IOException {
        ValueEncoding encoding = encodingByTypeName.get(encoded.getTypeName());
        if (null == encoding) {
            // If there is no encoding, simply embed the serialized value as an object.
            return encoded;
        } else {
            return encoding.decode(encoded.getEncoded());
        }
    }

    public Optional<SerializedValue> encode(Object decoded) throws IOException {
        if (decoded instanceof SerializedValue) {
            // One way a SerializedValue can enter the graph is if the value's type was not understood at decoding time.
            return Optional.of((SerializedValue) decoded);
        } else {
            // Note: subclass relationships are not taken into account.
            // The object must be a direct instance of the supported class.
            ValueEncoding encoding = encodingByClass.get(decoded.getClass());

            if (null == encoding) {
                return Optional.empty();
            } else {
                String encoded = encoding.encode(decoded);
                return Optional.of(new SerializedValue(encoding.getTypeName(), encoded));
            }
        }
    }
}
