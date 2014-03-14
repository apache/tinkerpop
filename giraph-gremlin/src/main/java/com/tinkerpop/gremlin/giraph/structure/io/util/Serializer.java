package com.tinkerpop.gremlin.giraph.structure.io.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Serializer {

    public static byte[] serializeObject(final Object object) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(outputStream);
        out.writeObject(object);
        out.close();
        return outputStream.toByteArray();
    }

    public static Object deserializeObject(final byte[] objectBytes) throws IOException, ClassNotFoundException {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(objectBytes);
        final ObjectInputStream in = new ObjectInputStream(inputStream);
        final Object object = in.readObject();
        in.close();
        return object;
    }
}
