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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.io.DefaultIo;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.javatuples.Pair;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link Graph} is a container object for a collection of {@link Vertex}, {@link Edge}, {@link VertexProperty},
 * and {@link Property} objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public interface Graph extends AutoCloseable {

    public static final String GRAPH = "gremlin.graph";

    /**
     * This should only be used by vendors to create keys, labels, etc. in a namespace safe from users.
     * Users are not allowed to generate property keys, step labels, etc. that are key'd "hidden".
     */
    public static class Hidden {

        /**
         * The prefix to denote that a key is a hidden key.
         */
        private static final String HIDDEN_PREFIX = "~";
        private static final int HIDDEN_PREFIX_LENGTH = HIDDEN_PREFIX.length();

        /**
         * Turn the provided key into a hidden key. If the key is already a hidden key, return key.
         *
         * @param key The key to make a hidden key
         * @return The hidden key
         */
        public static String hide(final String key) {
            return isHidden(key) ? key : HIDDEN_PREFIX.concat(key);
        }

        /**
         * Turn the provided hidden key into an non-hidden key. If the key is not a hidden key, return key.
         *
         * @param key The hidden key
         * @return The non-hidden representation of the key
         */
        public static String unHide(final String key) {
            return isHidden(key) ? key.substring(HIDDEN_PREFIX_LENGTH) : key;
        }

        /**
         * Determines whether the provided key is a hidden key or not.
         *
         * @param key The key to check for hidden status
         * @return Whether the provided key is a hidden key or not
         */
        public static boolean isHidden(final String key) {
            return key.startsWith(HIDDEN_PREFIX);
        }
    }

    public static <G extends Graph> G empty(final Class<G> graphClass) {
        try {
            return (G) graphClass.getMethod("empty").invoke(null);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Add a {@link Vertex} to the graph given an optional series of key/value pairs.  These key/values
     * must be provided in an even number where the odd numbered arguments are {@link String} property keys and the
     * even numbered arguments are the related property values.
     *
     * @param keyValues The key/value pairs to turn into vertex properties
     * @return The newly created vertex
     */
    public Vertex addVertex(final Object... keyValues);

    /**
     * Add a {@link Vertex} to the graph with provided vertex label.
     *
     * @param label the label of the vertex
     * @return The newly created labeled vertex
     */
    @Graph.Helper
    public default Vertex addVertex(final String label) {
        return this.addVertex(T.label, label);
    }

    /**
     * Declare the {@link GraphComputer} to use for OLAP operations on the graph.
     * If the graph does not support graph computer then an {@link java.lang.UnsupportedOperationException} is thrown.
     *
     * @param graphComputerClass The graph computer class to use.
     * @return A graph computer for processing this graph
     * @throws IllegalArgumentException if the provided {@link GraphComputer} class is not supported.
     */
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException;

    public GraphComputer compute() throws IllegalArgumentException;

    public default <C extends TraversalSource> C traversal(final TraversalSource.Builder<C> contextBuilder) {
        return contextBuilder.create(this);
    }

    public default GraphTraversalSource traversal() {
        return this.traversal(GraphTraversalSource.build().engine(StandardTraversalEngine.build()));
    }

    /**
     * Get the {@link Vertex} objects in this graph with the provided vertex ids. If no ids are provided, get all vertices.
     *
     * @param vertexIds the ids of the vertices to get
     * @return an {@link Iterator} of vertices that match the provided vertex ids
     */
    public Iterator<Vertex> vertices(final Object... vertexIds);

    /**
     * Get the {@link Edge} objects in this graph with the provided edge ids. If no ids are provided, get all edges.
     *
     * @param edgeIds the ids of the edges to get
     * @return an {@link Iterator} of edges that match the provided edge ids
     */
    public Iterator<Edge> edges(final Object... edgeIds);

    /**
     * Configure and control the transactions for those graphs that support this feature.
     */
    public Transaction tx();

    /**
     * Provide input/output methods for serializing graph data.
     */
    public default Io io() {
        return new DefaultIo(this);
    }

    /**
     * A collection of global {@link Variables} associated with the graph.
     * Variables are used for storing metadata about the graph.
     *
     * @return The variables associated with this graph
     */
    public Variables variables();

    /**
     * Get the {@link org.apache.commons.configuration.Configuration} associated with the construction of this graph.
     * Whatever configuration was passed to {@link org.apache.tinkerpop.gremlin.structure.util.GraphFactory#open(org.apache.commons.configuration.Configuration)}
     * is what should be returned by this method.
     *
     * @return the configuration used during graph construction.
     */
    public Configuration configuration();

    /**
     * Provides access to functions related to reading and writing graph data.  Implementers can override these
     * methods to provider mapper configurations to the default {@link org.apache.tinkerpop.gremlin.structure.io.GraphReader}
     * and {@link org.apache.tinkerpop.gremlin.structure.io.GraphWriter} implementations (i.e. to register mapper
     * serialization classes).
     */
    public interface Io {
        /**
         * Creates a {@link org.apache.tinkerpop.gremlin.structure.io.GraphReader} builder for Gryo serializations. This
         * method calls the {@link Io#gryoMapper} method to supply to
         * {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader.Builder#mapper} which means that implementers
         * should usually just override {@link Io#gryoMapper} to append in their mapper classes.
         */
        public default GryoReader.Builder gryoReader() {
            return GryoReader.build().mapper(gryoMapper().create());
        }

        /**
         * Creates a {@link org.apache.tinkerpop.gremlin.structure.io.GraphWriter} builder for Gryo serializations. This
         * method calls the {@link Io#gryoMapper} method to supply to
         * {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter.Builder#mapper} which means that implementers
         * should usually just override {@link Io#gryoMapper} to append in their mapper classes.
         */
        public default GryoWriter.Builder gryoWriter() {
            return GryoWriter.build().mapper(gryoMapper().create());
        }

        /**
         * Write a gryo file using the default configuration of the {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter}.
         */
        public void writeGryo(final String file) throws IOException;

        /**
         * Read a gryo file using the default configuration of the {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader}.
         */
        public void readGryo(final String file) throws IOException;

        /**
         * By default, this method creates an instance of the most current version of {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper} which is
         * used to serialize data to and from the graph.   Implementers with mapper classes (e.g. a non-primitive
         * class returned from {@link Element#id}) should override this method with those classes automatically
         * registered to the returned {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper}.
         * <br/>
         * Implementers should respect versions.  Once a class is registered, the order of its registration should be
         * maintained. Note that registering such classes will reduce the portability of the graph data as data
         * written with {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper} will not be readable without this serializer configuration.  It is
         * considered good practice to make serialization classes generally available so that users may
         * register these classes themselves if necessary when building up a mapper {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper}
         * instance.
         * <br/>
         * Note that this method is meant to return current versions for serialization operations.  Users wishing
         * to use an "older" version should construct this instance as well as their readers and writers manually.
         */
        public default GryoMapper.Builder gryoMapper() {
            return GryoMapper.build();
        }

        /**
         * Creates a {@link org.apache.tinkerpop.gremlin.structure.io.GraphReader} builder for GraphML serializations. GraphML
         * is the most portable of all the formats, but comes at the price of the least flexibility.
         * {@code Graph} implementations that have mapper classes that need to be serialized will not be able
         * to properly use this format effectively.
         */
        public default GraphMLReader.Builder graphMLReader() {
            return GraphMLReader.build();
        }

        /**
         * Creates a {@link org.apache.tinkerpop.gremlin.structure.io.GraphWriter} builder for GraphML serializations. GraphML
         * is the most portable of all the formats, but comes at the price of the least flexibility.
         * {@code Graph} implementations that have mapper classes that need to be serialized will not be able
         * to properly use this format effectively.
         */
        public default GraphMLWriter.Builder graphMLWriter() {
            return GraphMLWriter.build();
        }

        /**
         * Write a GraphML file using the default configuration of the {@link GraphMLWriter}.
         */
        public void writeGraphML(final String file) throws IOException;

        /**
         * Read a GraphML file using the default configuration of the {@link GraphMLReader}.
         */
        public void readGraphML(final String file) throws IOException;

        /**
         * Creates a {@link org.apache.tinkerpop.gremlin.structure.io.GraphReader} builder for GraphSON serializations.
         * GraphSON is forgiving for implementers and will typically do a "reasonable" job in serializing most
         * mapper classes.  This method by default uses the {@link org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper} created by
         * {@link #graphSONMapper}.  That method enables implementers to register mapper serialization
         * modules for classes that do not serialize nicely by the default JSON serializers or completely
         * fail to do so.
         */
        public default GraphSONReader.Builder graphSONReader() {
            return GraphSONReader.build().mapper(graphSONMapper().create());
        }

        /**
         * Creates a {@link org.apache.tinkerpop.gremlin.structure.io.GraphWriter} builder for GraphML serializations.
         * GraphSON is forgiving for implementers and will typically do a "reasonable" job in serializing most
         * mapper classes.  This method by default uses the {@link org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper} created by
         * {@link #graphSONMapper}. That method enables implementers to register mapper serialization
         * modules for classes that do not serialize nicely by the default JSON serializers or completely
         * fail to do so.
         */
        public default GraphSONWriter.Builder graphSONWriter() {
            return GraphSONWriter.build().mapper(graphSONMapper().create());
        }

        /**
         * Write a GraphSON file using the default configuration of the {@link GraphSONWriter}.
         */
        public void writeGraphSON(final String file) throws IOException;

        /**
         * Read a GraphSON file using the default configuration of the {@link GraphSONReader}.
         */
        public void readGraphSON(final String file) throws IOException;

        /**
         * By default, this method creates an instance of the most current version of
         * {@link org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper.Builder} which is can produce a
         * {@link org.apache.tinkerpop.gremlin.structure.io.Mapper} implementation for GraphSON to
         * serialize data to and from the graph.   Implementers with custom classes (e.g. a
         * non-primitive class returned from {@link Element#id}) should override this method with serialization
         * modules added.
         * <br/>
         * It is considered good practice to make serialization classes generally available so that users may
         * register these classes themselves if necessary when building up a mapper {@link org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper}
         * instance.
         * <br/>
         * Note that this method is meant to return a {@link org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper.Builder} with default configuration
         * for the current {@link Graph}.  Users can adjust and override such settings by altering the builder
         * settings.
         */
        public default GraphSONMapper.Builder graphSONMapper() {
            return GraphSONMapper.build();
        }
    }

    /**
     * Graph variables are a set of key/value pairs associated with the graph.The keys are String and the values
     * are Objects.
     */
    public interface Variables {

        /**
         * Keys set for the available variables.
         */
        public Set<String> keys();

        /**
         * Gets a variable.
         */
        public <R> Optional<R> get(final String key);

        /**
         * Sets a variable.
         */
        public void set(final String key, Object value);

        /**
         * Removes a variable.
         */
        public void remove(final String key);

        /**
         * Gets the variables of the {@link Graph} as a {@code Map}.
         */
        @Graph.Helper
        public default Map<String, Object> asMap() {
            final Map<String, Object> map = keys().stream()
                    .map(key -> Pair.with(key, get(key).get()))
                    .collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));
            return Collections.unmodifiableMap(map);
        }

        public static class Exceptions {

            public static IllegalArgumentException variableKeyCanNotBeEmpty() {
                return new IllegalArgumentException("Graph variable key can not be the empty string");
            }

            public static IllegalArgumentException variableKeyCanNotBeNull() {
                return new IllegalArgumentException("Graph variable key can not be null");
            }

            public static IllegalArgumentException variableValueCanNotBeNull() {
                return new IllegalArgumentException("Graph variable value can not be null");
            }

            public static UnsupportedOperationException dataTypeOfVariableValueNotSupported(final Object val) {
                return new UnsupportedOperationException(String.format("Graph variable value [%s] is of type %s is not supported", val, val.getClass()));
            }
        }

    }

    /**
     * Gets the {@link Features} exposed by the underlying {@code Graph} implementation.
     */
    public default Features features() {
        return new Features() {
        };
    }

    /**
     * An interface that represents the capabilities of a {@code Graph} implementation.  By default all methods
     * of features return {@code true} and it is up to implementers to disable feature they don't support.  Users
     * should check features prior to using various functions of TinkerPop to help ensure code portability
     * across implementations.  For example, a common usage would be to check if a graph supports transactions prior
     * to calling the commit method on {@link #tx()}.
     */
    public interface Features {

        /**
         * Gets the features related to "graph" operation.
         */
        public default GraphFeatures graph() {
            return new GraphFeatures() {
            };
        }

        /**
         * Gets the features related to "vertex" operation.
         */
        public default VertexFeatures vertex() {
            return new VertexFeatures() {
            };
        }

        /**
         * Gets the features related to "edge" operation.
         */
        public default EdgeFeatures edge() {
            return new EdgeFeatures() {
            };
        }

        /**
         * Features specific to a operations of a "graph".
         */
        public interface GraphFeatures extends FeatureSet {
            public static final String FEATURE_COMPUTER = "Computer";
            public static final String FEATURE_TRANSACTIONS = "Transactions";
            public static final String FEATURE_PERSISTENCE = "Persistence";
            public static final String FEATURE_THREADED_TRANSACTIONS = "ThreadedTransactions";

            /**
             * Determines if the {@code Graph} implementation supports
             * {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer} based processing.
             */
            @FeatureDescriptor(name = FEATURE_COMPUTER)
            public default boolean supportsComputer() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports persisting it's contents natively to disk.
             * This feature does not refer to every graph's ability to write to disk via the Gremlin IO packages
             * (.e.g. GraphML), unless the graph natively persists to disk via those options somehow.  For example,
             * TinkerGraph does not support this feature as it is a pure in-sideEffects graph.
             */
            @FeatureDescriptor(name = FEATURE_PERSISTENCE)
            public default boolean supportsPersistence() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementations supports transactions.
             */
            @FeatureDescriptor(name = FEATURE_TRANSACTIONS)
            public default boolean supportsTransactions() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports threaded transactions which allow a transaction
             * to be executed across multiple threads via {@link org.apache.tinkerpop.gremlin.structure.Transaction#create()}.
             */
            @FeatureDescriptor(name = FEATURE_THREADED_TRANSACTIONS)
            public default boolean supportsThreadedTransactions() {
                return true;
            }

            /**
             * Gets the features related to "graph sideEffects" operation.
             */
            public default VariableFeatures variables() {
                return new VariableFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Vertex} operations.
         */
        public interface VertexFeatures extends ElementFeatures {
            public static final String FEATURE_ADD_VERTICES = "AddVertices";
            public static final String FEATURE_MULTI_PROPERTIES = "MultiProperties";
            public static final String FEATURE_META_PROPERTIES = "MetaProperties";
            public static final String FEATURE_REMOVE_VERTICES = "RemoveVertices";

            /**
             * Determines if a {@link Vertex} can be added to the {@code Graph}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_VERTICES)
            public default boolean supportsAddVertices() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can be removed from the {@code Graph}.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_VERTICES)
            public default boolean supportsRemoveVertices() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can support multiple properties with the same key.
             */
            @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
            public default boolean supportsMultiProperties() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can support properties on vertex properties.  It is assumed that a
             * graph will support all the same data types for meta-properties that are supported for regular
             * properties.
             */
            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            public default boolean supportsMetaProperties() {
                return true;
            }

            /**
             * Gets features related to "properties" on a {@link Vertex}.
             */
            public default VertexPropertyFeatures properties() {
                return new VertexPropertyFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Edge} operations.
         */
        public interface EdgeFeatures extends ElementFeatures {
            public static final String FEATURE_ADD_EDGES = "AddEdges";
            public static final String FEATURE_REMOVE_EDGES = "RemoveEdges";

            /**
             * Determines if an {@link Edge} can be added to a {@code Vertex}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_EDGES)
            public default boolean supportsAddEdges() {
                return true;
            }

            /**
             * Determines if an {@link Edge} can be removed from a {@code Vertex}.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_EDGES)
            public default boolean supportsRemoveEdges() {
                return true;
            }

            /**
             * Gets features related to "properties" on an {@link Edge}.
             */
            public default EdgePropertyFeatures properties() {
                return new EdgePropertyFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Element} objects.  This is a base interface.
         */
        public interface ElementFeatures extends FeatureSet {
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            public static final String FEATURE_NUMERIC_IDS = "NumericIds";
            public static final String FEATURE_STRING_IDS = "StringIds";
            public static final String FEATURE_UUID_IDS = "UuidIds";
            public static final String FEATURE_CUSTOM_IDS = "CustomIds";
            public static final String FEATURE_ANY_IDS = "AnyIds";
            public static final String FEATURE_ADD_PROPERTY = "AddProperty";
            public static final String FEATURE_REMOVE_PROPERTY = "RemoveProperty";

            /**
             * Determines if an {@link Element} allows properties to be added.  This feature is set independently from
             * supporting "data types" and refers to support of calls to {@link Element#property(String, Object)}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_PROPERTY)
            public default boolean supportsAddProperty() {
                return true;
            }

            /**
             * Determines if an {@link Element} allows properties to be removed.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            public default boolean supportsRemoveProperty() {
                return true;
            }

            /**
             * Determines if an {@link Element} can have a user defined identifier.  Implementation that do not support
             * this feature will be expected to auto-generate unique identifiers.
             */
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has numeric identifiers.
             */
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            public default boolean supportsNumericIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has string identifiers.
             */
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public default boolean supportsStringIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has UUID identifiers.
             */
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public default boolean supportsUuidIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has mapper identifiers where "mapper" refers to an implementation
             * defined object.
             */
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public default boolean supportsCustomIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} any Java object is a suitable identifier.
             */
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public default boolean supportsAnyIds() {
                return true;
            }
        }

        /**
         * Features that are related to {@link Vertex} {@link Property} objects.
         */
        public interface VertexPropertyFeatures extends PropertyFeatures {
            public static final String FEATURE_ADD_PROPERTY = "AddProperty";
            public static final String FEATURE_REMOVE_PROPERTY = "RemoveProperty";
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            public static final String FEATURE_NUMERIC_IDS = "NumericIds";
            public static final String FEATURE_STRING_IDS = "StringIds";
            public static final String FEATURE_UUID_IDS = "UuidIds";
            public static final String FEATURE_CUSTOM_IDS = "CustomIds";
            public static final String FEATURE_ANY_IDS = "AnyIds";

            /**
             * Determines if a {@link VertexProperty} allows properties to be added.
             */
            @FeatureDescriptor(name = FEATURE_ADD_PROPERTY)
            public default boolean supportsAddProperty() {
                return true;
            }

            /**
             * Determines if a {@link VertexProperty} allows properties to be removed.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            public default boolean supportsRemoveProperty() {
                return true;
            }

            /**
             * Determines if a {@link VertexProperty} allows an identifier to be assigned to it.
             */
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has numeric identifiers.
             */
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            public default boolean supportsNumericIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has string identifiers.
             */
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public default boolean supportsStringIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has UUID identifiers.
             */
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public default boolean supportsUuidIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has mapper identifiers where "mapper" refers to an implementation
             * defined object.
             */
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public default boolean supportsCustomIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} any Java object is a suitable identifier.
             */
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public default boolean supportsAnyIds() {
                return true;
            }
        }

        /**
         * Features that are related to {@link Edge} {@link Property} objects.
         */
        public interface EdgePropertyFeatures extends PropertyFeatures {
        }

        /**
         * A base interface for {@link Edge} or {@link Vertex} {@link Property} features.
         */
        public interface PropertyFeatures extends DataTypeFeatures {
            public static final String FEATURE_PROPERTIES = "Properties";

            /**
             * Determines if an {@link Element} allows for the processing of at least one data type defined by the
             * features.  In this case "processing" refers to at least "reading" the data type. If any of the
             * features on {@link PropertyFeatures} is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_PROPERTIES)
            public default boolean supportsProperties() {
                return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsSerializableValues()
                        || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                        || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                        || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
            }
        }

        /**
         * Features for {@link org.apache.tinkerpop.gremlin.structure.Graph.Variables}.
         */
        public interface VariableFeatures extends DataTypeFeatures {
            public static final String FEATURE_VARIABLES = "Variables";

            /**
             * If any of the features on {@link org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures} is
             * true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_VARIABLES)
            public default boolean supportsVariables() {
                return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsSerializableValues()
                        || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                        || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                        || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
            }
        }

        /**
         * Base interface for features that relate to supporting different data types.
         */
        public interface DataTypeFeatures extends FeatureSet {
            public static final String FEATURE_BOOLEAN_VALUES = "BooleanValues";
            public static final String FEATURE_BYTE_VALUES = "ByteValues";
            public static final String FEATURE_DOUBLE_VALUES = "DoubleValues";
            public static final String FEATURE_FLOAT_VALUES = "FloatValues";
            public static final String FEATURE_INTEGER_VALUES = "IntegerValues";
            public static final String FEATURE_LONG_VALUES = "LongValues";
            public static final String FEATURE_MAP_VALUES = "MapValues";
            public static final String FEATURE_MIXED_LIST_VALUES = "MixedListValues";
            public static final String FEATURE_BOOLEAN_ARRAY_VALUES = "BooleanArrayValues";
            public static final String FEATURE_BYTE_ARRAY_VALUES = "ByteArrayValues";
            public static final String FEATURE_DOUBLE_ARRAY_VALUES = "DoubleArrayValues";
            public static final String FEATURE_FLOAT_ARRAY_VALUES = "FloatArrayValues";
            public static final String FEATURE_INTEGER_ARRAY_VALUES = "IntegerArrayValues";
            public static final String FEATURE_LONG_ARRAY_VALUES = "LongArrayValues";
            public static final String FEATURE_SERIALIZABLE_VALUES = "SerializableValues";
            public static final String FEATURE_STRING_ARRAY_VALUES = "StringArrayValues";
            public static final String FEATURE_STRING_VALUES = "StringValues";
            public static final String FEATURE_UNIFORM_LIST_VALUES = "UniformListValues";

            /**
             * Supports setting of a boolean value.
             */
            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            public default boolean supportsBooleanValues() {
                return true;
            }

            /**
             * Supports setting of a byte value.
             */
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public default boolean supportsByteValues() {
                return true;
            }

            /**
             * Supports setting of a double value.
             */
            @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
            public default boolean supportsDoubleValues() {
                return true;
            }

            /**
             * Supports setting of a float value.
             */
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public default boolean supportsFloatValues() {
                return true;
            }

            /**
             * Supports setting of a integer value.
             */
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public default boolean supportsIntegerValues() {
                return true;
            }

            /**
             * Supports setting of a long value.
             */
            @FeatureDescriptor(name = FEATURE_LONG_VALUES)
            public default boolean supportsLongValues() {
                return true;
            }

            /**
             * Supports setting of a {@code Map} value.  The assumption is that the {@code Map} can contain
             * arbitrary serializable values that may or may not be defined as a feature itself.
             */
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public default boolean supportsMapValues() {
                return true;
            }

            /**
             * Supports setting of a {@code List} value.  The assumption is that the {@code List} can contain
             * arbitrary serializable values that may or may not be defined as a feature itself.  As this
             * {@code List} is "mixed" it does not need to contain objects of the same type.
             *
             * @see #supportsMixedListValues()
             */
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public default boolean supportsMixedListValues() {
                return true;
            }

            /**
             * Supports setting of an array of boolean values.
             */
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public default boolean supportsBooleanArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of byte values.
             */
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public default boolean supportsByteArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of double values.
             */
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public default boolean supportsDoubleArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of float values.
             */
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public default boolean supportsFloatArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of integer values.
             */
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public default boolean supportsIntegerArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of string values.
             */
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public default boolean supportsStringArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of long values.
             */
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public default boolean supportsLongArrayValues() {
                return true;
            }

            /**
             * Supports setting of a Java serializable value.
             */
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public default boolean supportsSerializableValues() {
                return true;
            }

            /**
             * Supports setting of a long value.
             */
            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            public default boolean supportsStringValues() {
                return true;
            }

            /**
             * Supports setting of a {@code List} value.  The assumption is that the {@code List} can contain
             * arbitrary serializable values that may or may not be defined as a feature itself.  As this
             * {@code List} is "uniform" it must contain objects of the same type.
             *
             * @see #supportsMixedListValues()
             */
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public default boolean supportsUniformListValues() {
                return true;
            }
        }

        /**
         * A marker interface to identify any set of Features. There is no need to implement this interface.
         */
        public interface FeatureSet {
        }

        /**
         * Implementers should not override this method. Note that this method utilizes reflection to check for
         * feature support.
         */
        default boolean supports(final Class<? extends FeatureSet> featureClass, final String feature)
                throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            final Object instance;
            if (featureClass.equals(GraphFeatures.class))
                instance = this.graph();
            else if (featureClass.equals(VariableFeatures.class))
                instance = this.graph().variables();
            else if (featureClass.equals(VertexFeatures.class))
                instance = this.vertex();
            else if (featureClass.equals(VertexPropertyFeatures.class))
                instance = this.vertex().properties();
            else if (featureClass.equals(EdgeFeatures.class))
                instance = this.edge();
            else if (featureClass.equals(EdgePropertyFeatures.class))
                instance = this.edge().properties();
            else if (featureClass.equals(PropertyFeatures.class))
                throw new IllegalArgumentException(String.format(
                        "Do not reference PropertyFeatures directly in tests, utilize a specific instance: %s, %s",
                        EdgePropertyFeatures.class, VertexPropertyFeatures.class));
            else
                throw new IllegalArgumentException(String.format(
                        "Expecting featureClass to be a valid Feature instance and not %s", featureClass));

            return (Boolean) featureClass.getMethod("supports" + feature).invoke(instance);
        }
    }

    /**
     * Common exceptions to use with a graph.
     */
    public static class Exceptions {

        private static final boolean debug = Boolean.parseBoolean(java.lang.System.getenv().getOrDefault("gremlin.structure.debug", "false"));

        public static UnsupportedOperationException variablesNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph variables");
        }

        public static UnsupportedOperationException transactionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support transactions");
        }

        public static UnsupportedOperationException graphComputerNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph computer");
        }

        public static IllegalArgumentException traversalEngineNotSupported(final TraversalEngine engine) {
            return new IllegalArgumentException("Graph does not support the provided traversal engine: " + engine.getClass().getCanonicalName());
        }

        public static IllegalArgumentException graphDoesNotSupportProvidedGraphComputer(final Class graphComputerClass) {
            return new IllegalArgumentException("Graph does not support the provided graph computer: " + graphComputerClass.getSimpleName());
        }

        public static UnsupportedOperationException vertexAdditionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support adding vertices");
        }

        public static IllegalArgumentException vertexWithIdAlreadyExists(final Object id) {
            return new IllegalArgumentException(String.format("Vertex with id already exists: %s", id));
        }

        public static IllegalArgumentException edgeWithIdAlreadyExists(final Object id) {
            return new IllegalArgumentException(String.format("Edge with id already exists: %s", id));
        }

        public static IllegalArgumentException idArgsMustBeEitherIdOrElement() {
            return new IllegalArgumentException("id arguments must be either ids or Elements");
        }

        public static IllegalArgumentException argumentCanNotBeNull(final String argument) {
            return new IllegalArgumentException(String.format("The provided argument can not be null: %s", argument));
        }

        public static NoSuchElementException elementNotFound(final Class<? extends Element> elementClass, final Object id) {
            return (null == id) ?
                    new NoSuchElementException("The " + elementClass.getSimpleName().toLowerCase() + " with id null does not exist in the graph") :
                    new NoSuchElementException("The " + elementClass.getSimpleName().toLowerCase() + " with id " + id + " of type " + id.getClass().getSimpleName() + " does not exist in the graph");
        }

        public static IllegalArgumentException onlyOneOrNoGraphComputerClass() {
            return new IllegalArgumentException("Provide either one or no graph computer class");
        }
    }

    /**
     * Defines the test suite that the implementer has decided to support and represents publicly as "passing".
     * Marking the {@link Graph} instance with this class allows that particular test suite to run.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Repeatable(OptIns.class)
    @Inherited
    public @interface OptIn {
        public static String SUITE_STRUCTURE_STANDARD = "org.apache.tinkerpop.gremlin.structure.StructureStandardSuite";
        public static String SUITE_STRUCTURE_PERFORMANCE = "org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite";
        public static String SUITE_PROCESS_COMPUTER = "org.apache.tinkerpop.gremlin.process.ProcessComputerSuite";
        public static String SUITE_PROCESS_STANDARD = "org.apache.tinkerpop.gremlin.process.ProcessStandardSuite";
        public static String SUITE_PROCESS_PERFORMANCE = "org.apache.tinkerpop.gremlin.process.ProcessPerformanceSuite";
        public static String SUITE_GROOVY_PROCESS_STANDARD = "org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite";
        public static String SUITE_GROOVY_PROCESS_COMPUTER = "org.apache.tinkerpop.gremlin.process.GroovyProcessComputerSuite";
        public static String SUITE_GROOVY_ENVIRONMENT = "org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite";
        public static String SUITE_GROOVY_ENVIRONMENT_INTEGRATE = "org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite";
        public static String SUITE_GROOVY_ENVIRONMENT_PERFORMANCE = "org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentPerformanceSuite";

        /**
         * The test suite class to opt in to.
         */
        public String value();
    }

    /**
     * Holds a collection of {@link OptIn} enabling multiple {@link OptIn} to be applied to a
     * single suite.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface OptIns {
        OptIn[] value();
    }

    /**
     * Defines a test in the suite that the implementer does not want to run.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Repeatable(OptOuts.class)
    @Inherited
    public @interface OptOut {
        /**
         * The test class to opt out of. This may be set to a base class of a test as in the case of the Gremlin
         * process class of tests from which Gremlin flavors extend.  If the actual test class is an inner class
         * of then use a "$" as a separator between the outer class and inner class.
         */
        public String test();

        /**
         * The specific name of the test method to opt out of or asterisk to opt out of all methods in a
         * {@link #test}.
         */
        public String method();

        /**
         * The reason the implementation is opting out of this test.
         */
        public String reason();

        /**
         * For parameterized tests specify the name of the test itself without its "square brackets".
         */
        public String specific() default "";
    }

    /**
     * Holds a collection of {@link OptOut} enabling multiple {@link OptOut} to be applied to a
     * single suite.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface OptOuts {
        OptOut[] value();
    }

    /**
     * Defines a method as a "helper method".  These methods will usually be default methods in the
     * core structure interfaces.  Any method marked with this annotation represent methods that should not
     * be implemented by vendors.  The test suite will enforce this convention and create a failure situation
     * if violated.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Inherited
    public @interface Helper {
    }
}
