package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ConfUtil {

    public static org.apache.commons.configuration.Configuration makeApacheConfiguration(final Configuration hadoopConfiguration) {
        final org.apache.commons.configuration.Configuration apacheConfiguration = new BaseConfiguration();
        hadoopConfiguration.iterator().forEachRemaining(e -> apacheConfiguration.setProperty(e.getKey(), e.getValue()));
        return apacheConfiguration;
    }

    public static Configuration makeHadoopConfiguration(final org.apache.commons.configuration.Configuration apacheConfiguration) {
        final Configuration hadoopConfiguration = new Configuration();
        apacheConfiguration.getKeys().forEachRemaining(key -> {
            final Object object = apacheConfiguration.getProperty(key);
            hadoopConfiguration.set(key, object.toString());
        });
        return hadoopConfiguration;
    }

    public static Class<InputFormat> getInputFormatFromVertexInputFormat(final Class<VertexInputFormat> vertexInputFormatClass) {
        try {
            if (GiraphGremlinInputFormat.class.isAssignableFrom(vertexInputFormatClass))
                return (((GiraphGremlinInputFormat) vertexInputFormatClass.getConstructor().newInstance()).getInputFormatClass());
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        throw new IllegalStateException("The provided VertexInputFormatClass is not a GiraphGremlinInputFormat");
    }

    public static void setByteArray(final Configuration configuration, final String key, final byte[] bytes) {
        final String[] byteStrings = new String[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            byteStrings[i] = Byte.toString(bytes[i]);
        }
        configuration.setStrings(key, byteStrings);
    }

    public static byte[] getByteArray(final Configuration configuration, final String key) {
        final String[] byteStrings = configuration.getStrings(key);
        final byte[] bytes = new byte[byteStrings.length];
        for (int i = 0; i < byteStrings.length; i++) {
            bytes[i] = Byte.valueOf(byteStrings[i]);
        }
        return bytes;
    }
}
