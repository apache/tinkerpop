package com.tinkerpop.gremlin.giraph.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SConfiguration extends BaseConfiguration implements Serializable {

    public SConfiguration() {

    }

    public SConfiguration(final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.setProperty(key, configuration.getProperty(key)));
    }
}
