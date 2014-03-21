package com.tinkerpop.gremlin.giraph.process.olap.util;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ConfUtil {

    public static org.apache.commons.configuration.Configuration apacheConfiguration(final Configuration hadoopConfiguration) {
        final org.apache.commons.configuration.Configuration apacheConfiguration = new BaseConfiguration();
        hadoopConfiguration.iterator().forEachRemaining(e -> apacheConfiguration.setProperty(e.getKey(), e.getValue()));
        return apacheConfiguration;
    }

    public static Configuration hadoopConfiguration(final org.apache.commons.configuration.Configuration apacheConfiguration) {
        final Configuration hadoopConfiguration = new Configuration();
        apacheConfiguration.getKeys().forEachRemaining(key -> {
            final Object object = apacheConfiguration.getProperty(key);
            hadoopConfiguration.set(key, object.toString());
        });
        return hadoopConfiguration;
    }
}
