/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.hadoop.structure;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopConfiguration extends AbstractConfiguration implements Serializable, Iterable {

    private final Map<String, Object> properties = new HashMap<>();

    public HadoopConfiguration() {
        super();
        super.setDelimiterParsingDisabled(true);
    }

    public HadoopConfiguration(final Configuration configuration) {
        this();
        this.copy(configuration);
    }

    @Override
    protected void addPropertyDirect(final String key, final Object value) {
        this.properties.put(convertKey(key), value);
    }

    @Override
    protected void clearPropertyDirect(final String key) {
        this.properties.remove(convertKey(key));
    }

    @Override
    public boolean isEmpty() {
        return this.properties.isEmpty();
    }

    @Override
    public boolean containsKey(final String key) {
        return this.properties.containsKey(convertKey(key));
    }

    @Override
    public Object getProperty(final String key) {
        return this.properties.get(convertKey(key));
    }

    @Override
    public Iterator<String> getKeys() {
        return this.properties.keySet().iterator();
    }

    @Deprecated
    private static String convertKey(final String key) {
        if (key.equals(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT))
            return Constants.GREMLIN_HADOOP_GRAPH_READER;
        else if (key.equals(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT))
            return Constants.GREMLIN_HADOOP_GRAPH_WRITER;
        else if (key.equals(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD))
            return Constants.GREMLIN_HADOOP_GRAPH_READER;
        else if (key.equals(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD))
            return Constants.GREMLIN_HADOOP_GRAPH_WRITER;
        else if (key.equals(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT_HAS_EDGES))
            return Constants.GREMLIN_HADOOP_GRAPH_READER_HAS_EDGES;
        else if (key.equals(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES))
            return Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES;
        else
            return key;
    }

    ///////

    public <A> Class<A> getGraphReader() {
        try {
            return (Class) Class.forName(this.getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public <A> Class<A> getGraphWriter() {
        try {
            return (Class) Class.forName(this.getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * @deprecated As of release 3.2.0, replaced by {@link HadoopConfiguration#getGraphReader()}.
     */
    @Deprecated
    public Class<InputFormat<NullWritable, VertexWritable>> getGraphInputFormat() {
        try {
            return (Class) Class.forName(this.getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * @deprecated As of release 3.2.0, replaced by {@link HadoopConfiguration#getGraphWriter()}.
     */
    @Deprecated
    public Class<OutputFormat<NullWritable, VertexWritable>> getGraphOutputFormat() {
        try {
            return (Class) Class.forName(this.getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getInputLocation() {
        return this.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION);
    }

    public void setInputLocation(final String inputLocation) {
        this.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputLocation);
    }

    public String getOutputLocation() {
        return this.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
    }

    public void setOutputLocation(final String outputLocation) {
        this.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
    }

    @Override
    public Iterator iterator() {
        return IteratorUtils.map(this.getKeys(), k -> new Pair<>(k, this.getProperty(k)));
    }
}