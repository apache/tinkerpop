package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.io.VertexInputFormat;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphConfiguration extends BaseConfiguration implements Serializable {

    public GiraphConfiguration() {

    }

    public GiraphConfiguration(final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.setProperty(key, configuration.getProperty(key)));
    }

    public Class<VertexInputFormat> getInputFormat() {
        try {
            return (Class) Class.forName(this.getString(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Class<VertexInputFormat> getOutputFormat() {
        try {
            return (Class) Class.forName(this.getString(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getInputLocation() {
        return this.getString(Constants.GREMLIN_INPUT_LOCATION);
    }

    public String getOutputLocation() {
        return this.getString(Constants.GREMLIN_OUTPUT_LOCATION);
    }
}
