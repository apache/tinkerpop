package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphConfiguration extends BaseConfiguration implements Serializable, Iterable {

    public GiraphConfiguration() {

    }

    public GiraphConfiguration(final Configuration configuration) {
        this.copy(configuration);
    }

    public Class<VertexInputFormat> getInputFormat() {
        try {
            return (Class) Class.forName(this.getString(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void setInputFormat(final Class<VertexInputFormat> inputFormatClass) {
        this.setProperty(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, inputFormatClass);
    }

    public Class<VertexInputFormat> getOutputFormat() {
        try {
            return (Class) Class.forName(this.getString(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void setOutputFormat(final Class<VertexOutputFormat> outputFormatClass) {
        this.setProperty(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS, outputFormatClass);
    }

    public String getInputLocation() {
        return this.getString(Constants.GREMLIN_GIRAPH_INPUT_LOCATION);
    }

    public void setInputLocation(final String inputLocation) {
        this.setProperty(Constants.GREMLIN_GIRAPH_INPUT_LOCATION, inputLocation);
    }

    public String getOutputLocation() {
        return this.getString(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION);
    }

    public void setOutputLocation(final String outputLocation) {
        this.setProperty(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION, outputLocation);
    }

    @Override
    public Iterator iterator() {
        return StreamFactory.stream(this.getKeys()).map(k -> new Pair(k, this.getProperty(k))).iterator();
    }
}
