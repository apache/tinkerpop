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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.Host;
import org.javatuples.Pair;

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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A {@link Graph} is a container object for a collection of {@link Vertex}, {@link Edge}, {@link VertexProperty},
 * and {@link Property} objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public interface Graph extends AutoCloseable, Host {

    /**
     * Configuration key used by {@link GraphFactory}} to determine which graph to instantiate.
     */
    String GRAPH = "gremlin.graph";

    /**
     * This should only be used by providers to create keys, labels, etc. in a namespace safe from users.
     * Users are not allowed to generate property keys, step labels, etc. that are key'd "hidden".
     */
    class Hidden {

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

    /**
     * Add a {@link Vertex} to the graph given an optional series of key/value pairs.  These key/values
     * must be provided in an even number where the odd numbered arguments are {@link String} property keys and the
     * even numbered arguments are the related property values.
     *
     * @param keyValues The key/value pairs to turn into vertex properties
     * @return The newly created vertex
     */
    Vertex addVertex(final Object... keyValues);

    /**
     * Add a {@link Vertex} to the graph with provided vertex label.
     *
     * @param label the label of the vertex
     * @return The newly created labeled vertex
     */
    default Vertex addVertex(final String label) {
        return this.addVertex(T.label, label);
    }

    /**
     * Declare the {@link GraphComputer} to use for OLAP operations on the graph.
     * If the graph does not support graph computer then an {@code UnsupportedOperationException} is thrown.
     *
     * @param graphComputerClass The graph computer class to use.
     * @return A graph computer for processing this graph
     * @throws IllegalArgumentException if the provided {@link GraphComputer} class is not supported.
     */
    <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException;

    /**
     * Generate a {@link GraphComputer} using the default engine of the underlying graph system.
     * This is a shorthand method for the more involved method that uses {@link Graph#compute(Class)}.
     *
     * @return A default graph computer
     * @throws IllegalArgumentException if there is no default graph computer
     */
    GraphComputer compute() throws IllegalArgumentException;

    /**
     * Generate a {@link TraversalSource} using the specified {@code TraversalSource} class.
     * The reusable {@link TraversalSource} provides methods for spawning {@link Traversal} instances.
     *
     * @param traversalSourceClass The traversal source class
     * @param <C>                  The traversal source class
     */
    default <C extends TraversalSource> C traversal(final Class<C> traversalSourceClass) {
        try {
            return traversalSourceClass.getConstructor(Graph.class).newInstance(this);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Generate a reusable {@link GraphTraversalSource} instance.
     * The {@link GraphTraversalSource} provides methods for creating {@link GraphTraversal} instances.
     *
     * @return A graph traversal source
     */
    default GraphTraversalSource traversal() {
        return new GraphTraversalSource(this);
    }

    /**
     * Get the {@link Vertex} objects in this graph with the provided vertex ids or {@link Vertex} objects themselves.
     * If no ids are provided, get all vertices.  Note that a vertex identifier does not need to correspond to the
     * actual id used in the graph.  It needs to be a bit more flexible than that in that given the
     * {@link Graph.Features} around id support, multiple arguments might be applicable here.
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsNumericIds()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * <li>g.vertices(1)</li>
     * <li>g.vertices(1L)</li>
     * <li>g.vertices(1.0d)</li>
     * <li>g.vertices(1.0f)</li>
     * <li>g.vertices("1")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsCustomIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * <li>g.vertices(v.id().toString())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsAnyIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * </ul>
     * <p/>                                                                                                         
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id().toString())</li>
     * <li>g.vertices("id")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id().toString())</li>
     * <li>g.vertices("id")</li>
     * </ul>
     *
     * @param vertexIds the ids of the vertices to get
     * @return an {@link Iterator} of vertices that match the provided vertex ids
     */
    Iterator<Vertex> vertices(final Object... vertexIds);

    /**
     * Get the {@link Edge} objects in this graph with the provided edge ids or {@link Edge} objects. If no ids are
     * provided, get all edges. Note that an edge identifier does not need to correspond to the actual id used in the
     * graph.  It needs to be a bit more flexible than that in that given the {@link Graph.Features} around id support,
     * multiple arguments might be applicable here.
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsNumericIds()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * <li>g.edges(1)</li>
     * <li>g.edges(1L)</li>
     * <li>g.edges(1.0d)</li>
     * <li>g.edges(1.0f)</li>
     * <li>g.edges("1")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsCustomIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * <li>g.edges(e.id().toString())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsAnyIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id().toString())</li>
     * <li>g.edges("id")</li>
     * </ul>
     *
     * @param edgeIds the ids of the edges to get
     * @return an {@link Iterator} of edges that match the provided edge ids
     */
    Iterator<Edge> edges(final Object... edgeIds);

    /**
     * Configure and control the transactions for those graphs that support this feature.
     */
    Transaction tx();

    /**
     * Configure and control the transactions for those graphs that support this feature. Graphs that support multiple
     * transaction models can use this method expose different sorts of {@link Transaction} implementations.
     */
    default <Tx extends Transaction> Tx tx(final Class<Tx> txClass) {
        throw new UnsupportedOperationException("This Graph does not support multiple transaction types - use tx() instead");
    }

    /**
     * Closing a {@code Graph} is equivalent to "shutdown" and implies that no further operations can be executed on
     * the instance.  Users should consult the documentation of the underlying graph database implementation for what
     * this "shutdown" will mean in general and, if supported, how open transactions are handled.  It will typically
     * be the end user's responsibility to synchronize the thread that calls {@code close()} with other threads that
     * are accessing open transactions. In other words, be sure that all work performed on the {@code Graph} instance
     * is complete prior to calling this method.
     * <p/>
     * TinkerPop does not enforce any particular semantics with respect to "shutdown". It is up to the graph provider
     * to decide what this method will do.
     */
    @Override
    void close() throws Exception;

    /**
     * Construct a particular {@link Io} implementation for reading and writing the {@code Graph} and other data.
     * End-users will "select" the {@link Io} implementation that they want to use by supplying the
     * {@link Io.Builder} that constructs it.  In this way, {@code Graph} vendors can supply their {@link IoRegistry}
     * to that builder thus allowing for custom serializers to be auto-configured into the {@link Io} instance.
     * Registering custom serializers is particularly useful for those  graphs that have complex types for
     * {@link Element} identifiers.
     * </p>
     * For those graphs that do not need to register any custom serializers, the default implementation should suffice.
     * If the default is overridden, take care to register the current graph via the
     * {@link org.apache.tinkerpop.gremlin.structure.io.Io.Builder#graph(Graph)} method.
     *
     * @deprecated As of release 3.4.0, partially replaced by {@link GraphTraversalSource#io(String)}. Notice
     * {@link GraphTraversalSource#io(String)} doesn't support read operation from {@code java.io.InputStream}
     * or write operation to {@code java.io.OutputStream}. Thus for readers or writers which need this functionality
     * are safe to use this deprecated method. There is no intention to remove this method unless all the
     * functionality is replaced by the `io` step of {@link GraphTraversalSource}.
     */
    @Deprecated
    default <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).create();
    }

    /**
     * A collection of global {@link Variables} associated with the graph.
     * Variables are used for storing metadata about the graph.
     *
     * @return The variables associated with this graph
     */
    Variables variables();

    /**
     * Get the {@code Configuration} associated with the construction of this graph. Whatever configuration was passed
     * to {@link GraphFactory#open(Configuration)} is what should be returned by this method.
     *
     * @return the configuration used during graph construction.
     */
    Configuration configuration();

    /**
     * Graph variables are a set of key/value pairs associated with the graph. The keys are String and the values
     * are Objects.
     */
    interface Variables {

        /**
         * Keys set for the available variables.
         */
        Set<String> keys();

        /**
         * Gets a variable.
         */
        <R> Optional<R> get(final String key);

        /**
         * Sets a variable.
         */
        void set(final String key, Object value);

        /**
         * Removes a variable.
         */
        void remove(final String key);

        /**
         * Gets the variables of the {@link Graph} as a {@code Map}.
         */
        default Map<String, Object> asMap() {
            final Map<String, Object> map = keys().stream()
                    .map(key -> Pair.with(key, get(key).get()))
                    .collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));
            return Collections.unmodifiableMap(map);
        }

        class Exceptions {

            private Exceptions() {
            }

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
                return dataTypeOfVariableValueNotSupported(val, null);
            }

            public static UnsupportedOperationException dataTypeOfVariableValueNotSupported(final Object val, final Exception rootCause) {
                return new UnsupportedOperationException(String.format("Graph variable value [%s] is of type %s is not supported", val, val.getClass()), rootCause);
            }
        }

    }

    /**
     * Gets the {@link Features} exposed by the underlying {@code Graph} implementation.
     */
    default Features features() {
        return new Features() {
        };
    }

    /**
     * An interface that represents the capabilities of a {@code Graph} implementation.  By default all methods
     * of features return {@code true} and it is up to implementers to disable feature they don't support.  Users
     * should check features prior to using various functions of TinkerPop to help ensure code portability
     * across implementations.  For example, a common usage would be to check if a graph supports transactions prior
     * to calling the commit method on {@link #tx()}.
     * <p/>
     * As an additional notice to Graph Providers, feature methods will be used by the test suite to determine which
     * tests will be ignored and which will be executed, therefore proper setting of these features is essential to
     * maximizing the amount of testing performed by the suite. Further note, that these methods may be called by the
     * TinkerPop core code to determine what operations may be appropriately executed which will have impact on
     * features utilized by users.
     */
    interface Features {

        /**
         * Gets the features related to "graph" operation.
         */
        default GraphFeatures graph() {
            return new GraphFeatures() {
            };
        }

        /**
         * Gets the features related to "vertex" operation.
         */
        default VertexFeatures vertex() {
            return new VertexFeatures() {
            };
        }

        /**
         * Gets the features related to "edge" operation.
         */
        default EdgeFeatures edge() {
            return new EdgeFeatures() {
            };
        }

        /**
         * Features specific to a operations of a "graph".
         */
        interface GraphFeatures extends FeatureSet {
            String FEATURE_COMPUTER = "Computer";
            String FEATURE_TRANSACTIONS = "Transactions";
            String FEATURE_PERSISTENCE = "Persistence";
            String FEATURE_THREADED_TRANSACTIONS = "ThreadedTransactions";
            String FEATURE_CONCURRENT_ACCESS = "ConcurrentAccess";
            String FEATURE_IO_READ = "IoRead";
            String FEATURE_IO_WRITE = "IoWrite";
            String FEATURE_ORDERABILITY_SEMANTICS = "OrderabilitySemantics";

            /**
             * Determines if the {@code Graph} implementation supports {@link GraphComputer} based processing.
             */
            @FeatureDescriptor(name = FEATURE_COMPUTER)
            default boolean supportsComputer() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports persisting it's contents natively to disk.
             * This feature does not refer to every graph's ability to write to disk via the Gremlin IO packages
             * (.e.g. GraphML), unless the graph natively persists to disk via those options somehow.  For example,
             * TinkerGraph does not support this feature as it is a pure in-sideEffects graph.
             */
            @FeatureDescriptor(name = FEATURE_PERSISTENCE)
            default boolean supportsPersistence() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports more than one connection to the same instance
             * at the same time.  For example, Neo4j embedded does not support this feature because concurrent
             * access to the same database files by multiple instances is not possible.  However, Neo4j HA could
             * support this feature as each new {@code Graph} instance coordinates with the Neo4j cluster allowing
             * multiple instances to operate on the same database.
             */
            @FeatureDescriptor(name = FEATURE_CONCURRENT_ACCESS)
            default boolean supportsConcurrentAccess() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementations supports transactions.
             */
            @FeatureDescriptor(name = FEATURE_TRANSACTIONS)
            default boolean supportsTransactions() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports threaded transactions which allow a transaction
             * to be executed across multiple threads via {@link Transaction#createThreadedTx()}.
             */
            @FeatureDescriptor(name = FEATURE_THREADED_TRANSACTIONS)
            default boolean supportsThreadedTransactions() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementations supports read operations as executed with the
             * {@link GraphTraversalSource#io(String)} step. Graph implementations will generally support this by
             * default as any graph that can support direct mutation through the Structure API will by default
             * accept data from the standard TinkerPop {@link GraphReader} implementations. However, some graphs like
             * {@code HadoopGraph} don't accept direct mutations but can still do reads from that {@code io()} step.
             */
            @FeatureDescriptor(name = FEATURE_IO_READ)
            default boolean supportsIoRead() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementations supports write operations as executed with the
             * {@link GraphTraversalSource#io(String)} step. Graph implementations will generally support this by
             * default given the standard TinkerPop {@link GraphWriter} implementations. However, some graphs like
             * {@code HadoopGraph} will use a different approach to handle writes.
             */
            @FeatureDescriptor(name = FEATURE_IO_WRITE)
            default boolean supportsIoWrite() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports total universal orderability per the Gremlin
             * orderability semantics.
             */
            @FeatureDescriptor(name = FEATURE_ORDERABILITY_SEMANTICS)
            default boolean supportsOrderabilitySemantics() {
                return true;
            }

            /**
             * Gets the features related to "graph sideEffects" operation.
             */
            default VariableFeatures variables() {
                return new VariableFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Vertex} operations.
         */
        interface VertexFeatures extends ElementFeatures {
            String FEATURE_ADD_VERTICES = "AddVertices";
            String FEATURE_MULTI_PROPERTIES = "MultiProperties";
            String FEATURE_DUPLICATE_MULTI_PROPERTIES = "DuplicateMultiProperties";
            String FEATURE_META_PROPERTIES = "MetaProperties";
            String FEATURE_REMOVE_VERTICES = "RemoveVertices";
            String FEATURE_UPSERT = "Upsert";

            /**
             * Gets the {@link VertexProperty.Cardinality} for a key.  By default, this method will return
             * {@link VertexProperty.Cardinality#list}.  Implementations that employ a schema can consult it to
             * determine the {@link VertexProperty.Cardinality}.  Those that do no have a schema can return their
             * default {@link VertexProperty.Cardinality} for every key.
             * <p/>
             * Note that this method is primarily used by TinkerPop for internal usage and may not be suitable to
             * reliably determine the cardinality of a key. For some implementation it may offer little more than a
             * hint on the actual cardinality. Generally speaking it is likely best to drop down to the API of the
             * {@link Graph} implementation for any schema related queries.
             */
            default VertexProperty.Cardinality getCardinality(final String key) {
                return VertexProperty.Cardinality.list;
            }

            /**
             * Determines if a {@link Vertex} can be added to the {@code Graph}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_VERTICES)
            default boolean supportsAddVertices() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can be removed from the {@code Graph}.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_VERTICES)
            default boolean supportsRemoveVertices() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can support multiple properties with the same key.
             */
            @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
            default boolean supportsMultiProperties() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can support non-unique values on the same key. For this value to be
             * {@code true}, then {@link #supportsMetaProperties()} must also return true. By default this method,
             * just returns what {@link #supportsMultiProperties()} returns.
             */
            @FeatureDescriptor(name = FEATURE_DUPLICATE_MULTI_PROPERTIES)
            default boolean supportsDuplicateMultiProperties() {
                return supportsMultiProperties();
            }

            /**
             * Determines if a {@link Vertex} can support properties on vertex properties.  It is assumed that a
             * graph will support all the same data types for meta-properties that are supported for regular
             * properties.
             */
            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            default boolean supportsMetaProperties() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation uses upsert functionality as opposed to insert
             * functionality for {@link #addVertex(String)}. This feature gives graph providers some flexibility as
             * to how graph mutations are treated. For graph providers, testing of this feature (as far as TinkerPop
             * is concerned) only covers graphs that can support user supplied identifiers as there is no other way
             * for TinkerPop to know what aspect of a vertex is unique to appropriately apply assertions. Graph
             * providers, especially those who support schema features, may have other methods for uniquely identifying
             * a vertex and should therefore resort to their own body of tests to validate this feature.
             */
            @FeatureDescriptor(name = FEATURE_UPSERT)
            default boolean supportsUpsert() {
                return false;
            }

            /**
             * Gets features related to "properties" on a {@link Vertex}.
             */
            default VertexPropertyFeatures properties() {
                return new VertexPropertyFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Edge} operations.
         */
        interface EdgeFeatures extends ElementFeatures {
            String FEATURE_ADD_EDGES = "AddEdges";
            String FEATURE_REMOVE_EDGES = "RemoveEdges";
            String FEATURE_UPSERT = "Upsert";

            /**
             * Determines if an {@link Edge} can be added to a {@code Vertex}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_EDGES)
            default boolean supportsAddEdges() {
                return true;
            }

            /**
             * Determines if an {@link Edge} can be removed from a {@code Vertex}.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_EDGES)
            default boolean supportsRemoveEdges() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation uses upsert functionality as opposed to insert
             * functionality for {@link Vertex#addEdge(String, Vertex, Object...)}. This feature gives graph providers
             * some flexibility as to how graph mutations are treated. For graph providers, testing of this feature
             * (as far as TinkerPop is concerned) only covers graphs that can support user supplied identifiers as
             * there is no other way for TinkerPop to know what aspect of a edge is unique to appropriately apply
             * assertions. Graph providers, especially those who support schema features, may have other methods for
             * uniquely identifying a edge and should therefore resort to their own body of tests to validate this
             * feature.
             */
            @FeatureDescriptor(name = FEATURE_UPSERT)
            default boolean supportsUpsert() {
                return false;
            }

            /**
             * Gets features related to "properties" on an {@link Edge}.
             */
            default EdgePropertyFeatures properties() {
                return new EdgePropertyFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Element} objects.  This is a base interface.
         */
        interface ElementFeatures extends FeatureSet {
            String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            String FEATURE_NUMERIC_IDS = "NumericIds";
            String FEATURE_STRING_IDS = "StringIds";
            String FEATURE_UUID_IDS = "UuidIds";
            String FEATURE_CUSTOM_IDS = "CustomIds";
            String FEATURE_ANY_IDS = "AnyIds";
            String FEATURE_ADD_PROPERTY = "AddProperty";
            String FEATURE_REMOVE_PROPERTY = "RemoveProperty";
            String FEATURE_NULL_PROPERTY_VALUES = "NullPropertyValues";

            /**
             * Determines if an {@link Element} allows properties with {@code null} property values. In the event that
             * this value is {@code false}, the underlying graph must treat {@code null} as an indication to remove
             * the property.
             */
            @FeatureDescriptor(name = FEATURE_NULL_PROPERTY_VALUES)
            default boolean supportsNullPropertyValues() {
                return true;
            }

            /**
             * Determines if an {@link Element} allows properties to be added.  This feature is set independently from
             * supporting "data types" and refers to support of calls to {@link Element#property(String, Object)}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_PROPERTY)
            default boolean supportsAddProperty() {
                return true;
            }

            /**
             * Determines if an {@link Element} allows properties to be removed.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            default boolean supportsRemoveProperty() {
                return true;
            }

            /**
             * Determines if an {@link Element} can have a user defined identifier.  Implementations that do not support
             * this feature will be expected to auto-generate unique identifiers.  In other words, if the {@link Graph}
             * allows {@code graph.addVertex(id,x)} to work and thus set the identifier of the newly added
             * {@link Vertex} to the value of {@code x} then this feature should return true.  In this case, {@code x}
             * is assumed to be an identifier data type that the {@link Graph} will accept.
             */
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            default boolean supportsUserSuppliedIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has numeric identifiers as their internal representation. In other
             * words, if the value returned from {@link Element#id()} is a numeric value then this method
             * should be return {@code true}.
             * <p/>
             * Note that this feature is most generally used for determining the appropriate tests to execute in the
             * Gremlin Test Suite.
             */
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            default boolean supportsNumericIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has string identifiers as their internal representation. In other
             * words, if the value returned from {@link Element#id()} is a string value then this method
             * should be return {@code true}.
             * <p/>
             * Note that this feature is most generally used for determining the appropriate tests to execute in the
             * Gremlin Test Suite.
             */
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            default boolean supportsStringIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has UUID identifiers as their internal representation. In other
             * words, if the value returned from {@link Element#id()} is a {@link UUID} value then this method
             * should be return {@code true}.
             * <p/>
             * Note that this feature is most generally used for determining the appropriate tests to execute in the
             * Gremlin Test Suite.
             */
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            default boolean supportsUuidIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has a specific custom object as their internal representation.
             * In other words, if the value returned from {@link Element#id()} is a type defined by the graph
             * implementations, such as OrientDB's {@code Rid}, then this method should be return {@code true}.
             * <p/>
             * Note that this feature is most generally used for determining the appropriate tests to execute in the
             * Gremlin Test Suite.
             */
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            default boolean supportsCustomIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} any Java object is a suitable identifier. TinkerGraph is a good
             * example of a {@link Graph} that can support this feature, as it can use any {@link Object} as
             * a value for the identifier.
             * <p/>
             * Note that this feature is most generally used for determining the appropriate tests to execute in the
             * Gremlin Test Suite. This setting should only return {@code true} if {@link #supportsUserSuppliedIds()}
             * is {@code true}.
             */
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            default boolean supportsAnyIds() {
                return true;
            }

            /**
             * Determines if an identifier will be accepted by the {@link Graph}.  This check is different than
             * what identifier internally supports as defined in methods like {@link #supportsNumericIds()}.  Those
             * refer to internal representation of the identifier.  A {@link Graph} may accept an identifier that
             * is not of those types and internally transform it to a native representation.
             * <p/>
             * Note that this method only applies if {@link #supportsUserSuppliedIds()} is {@code true}. Those that
             * return {@code false} for that method can immediately return false for this one as it allows no ids
             * of any type (it generates them all).
             * <p/>
             * The default implementation will immediately return {@code false} if {@link #supportsUserSuppliedIds()}
             * is {@code false}.  If custom identifiers are supported then it will throw an exception.  Those that
             * return {@code true} for {@link #supportsCustomIds()} should override this method. If
             * {@link #supportsAnyIds()} is {@code true} then the identifier will immediately be allowed.  Finally,
             * if any of the other types are supported, they will be typed checked against the class of the supplied
             * identifier.
             */
            default boolean willAllowId(final Object id) {
                if (!supportsUserSuppliedIds()) return false;
                if (supportsCustomIds())
                    throw new UnsupportedOperationException("The default implementation is not capable of validating custom ids - please override");

                return supportsAnyIds() || (supportsStringIds() && id instanceof String)
                        || (supportsNumericIds() && id instanceof Number) || (supportsUuidIds() && id instanceof UUID);
            }
        }

        /**
         * Features that are related to {@link Vertex} {@link Property} objects.
         */
        interface VertexPropertyFeatures extends PropertyFeatures {
            String FEATURE_REMOVE_PROPERTY = "RemoveProperty";
            String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            String FEATURE_NUMERIC_IDS = "NumericIds";
            String FEATURE_STRING_IDS = "StringIds";
            String FEATURE_UUID_IDS = "UuidIds";
            String FEATURE_CUSTOM_IDS = "CustomIds";
            String FEATURE_ANY_IDS = "AnyIds";
            String FEATURE_NULL_PROPERTY_VALUES = "NullPropertyValues";

            /**
             * Determines if meta-properties allow for {@code null} property values.
             */
            @FeatureDescriptor(name = FEATURE_NULL_PROPERTY_VALUES)
            default boolean supportsNullPropertyValues() {
                return true;
            }

            /**
             * Determines if a {@link VertexProperty} allows properties to be removed.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            default boolean supportsRemoveProperty() {
                return true;
            }

            /**
             * Determines if a {@link VertexProperty} allows an identifier to be assigned to it.
             */
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            default boolean supportsUserSuppliedIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has numeric identifiers as their internal representation.
             */
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            default boolean supportsNumericIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has string identifiers as their internal representation.
             */
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            default boolean supportsStringIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has UUID identifiers as their internal representation.
             */
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            default boolean supportsUuidIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has a specific custom object as their internal representation.
             */
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            default boolean supportsCustomIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} any Java object is a suitable identifier.  Note that this
             * setting can only return true if {@link #supportsUserSuppliedIds()} is true.
             */
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            default boolean supportsAnyIds() {
                return true;
            }

            /**
             * Determines if an identifier will be accepted by the {@link Graph}.  This check is different than
             * what identifier internally supports as defined in methods like {@link #supportsNumericIds()}.  Those
             * refer to internal representation of the identifier.  A {@link Graph} may accept an identifier that
             * is not of those types and internally transform it to a native representation.
             * <p/>
             * Note that this method only applies if {@link #supportsUserSuppliedIds()} is {@code true}. Those that
             * return {@code false} for that method can immediately return false for this one as it allows no ids
             * of any type (it generates them all).
             * <p/>
             * The default implementation will immediately return {@code false} if {@link #supportsUserSuppliedIds()}
             * is {@code false}.  If custom identifiers are supported then it will throw an exception.  Those that
             * return {@code true} for {@link #supportsCustomIds()} should override this method. If
             * {@link #supportsAnyIds()} is {@code true} then the identifier will immediately be allowed.  Finally,
             * if any of the other types are supported, they will be typed checked against the class of the supplied
             * identifier.
             */
            default boolean willAllowId(final Object id) {
                if (!supportsUserSuppliedIds()) return false;
                if (supportsCustomIds())
                    throw new UnsupportedOperationException("The default implementation is not capable of validating custom ids - please override");

                return supportsAnyIds() || (supportsStringIds() && id instanceof String)
                        || (supportsNumericIds() && id instanceof Number) || (supportsUuidIds() && id instanceof UUID);
            }
        }

        /**
         * Features that are related to {@link Edge} {@link Property} objects.
         */
        interface EdgePropertyFeatures extends PropertyFeatures {
        }

        /**
         * A base interface for {@link Edge} or {@link Vertex} {@link Property} features.
         */
        interface PropertyFeatures extends DataTypeFeatures {
            String FEATURE_PROPERTIES = "Properties";

            /**
             * Determines if an {@link Element} allows for the processing of at least one data type defined by the
             * features.  In this case "processing" refers to at least "reading" the data type. If any of the
             * features on {@link PropertyFeatures} is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_PROPERTIES)
            default boolean supportsProperties() {
                return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsSerializableValues()
                        || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                        || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                        || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
            }
        }

        /**
         * Features for {@link Graph.Variables}.
         */
        interface VariableFeatures extends DataTypeFeatures {
            String FEATURE_VARIABLES = "Variables";

            /**
             * If any of the features on {@link VariableFeatures} is {@code true} then this value must be {@code true}.
             */
            @FeatureDescriptor(name = FEATURE_VARIABLES)
            default boolean supportsVariables() {
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
        interface DataTypeFeatures extends FeatureSet {
            String FEATURE_BOOLEAN_VALUES = "BooleanValues";
            String FEATURE_BYTE_VALUES = "ByteValues";
            String FEATURE_DOUBLE_VALUES = "DoubleValues";
            String FEATURE_FLOAT_VALUES = "FloatValues";
            String FEATURE_INTEGER_VALUES = "IntegerValues";
            String FEATURE_LONG_VALUES = "LongValues";
            String FEATURE_MAP_VALUES = "MapValues";
            String FEATURE_MIXED_LIST_VALUES = "MixedListValues";
            String FEATURE_BOOLEAN_ARRAY_VALUES = "BooleanArrayValues";
            String FEATURE_BYTE_ARRAY_VALUES = "ByteArrayValues";
            String FEATURE_DOUBLE_ARRAY_VALUES = "DoubleArrayValues";
            String FEATURE_FLOAT_ARRAY_VALUES = "FloatArrayValues";
            String FEATURE_INTEGER_ARRAY_VALUES = "IntegerArrayValues";
            String FEATURE_LONG_ARRAY_VALUES = "LongArrayValues";
            String FEATURE_SERIALIZABLE_VALUES = "SerializableValues";
            String FEATURE_STRING_ARRAY_VALUES = "StringArrayValues";
            String FEATURE_STRING_VALUES = "StringValues";
            String FEATURE_UNIFORM_LIST_VALUES = "UniformListValues";

            /**
             * Supports setting of a boolean value.
             */
            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            default boolean supportsBooleanValues() {
                return true;
            }

            /**
             * Supports setting of a byte value.
             */
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            default boolean supportsByteValues() {
                return true;
            }

            /**
             * Supports setting of a double value.
             */
            @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
            default boolean supportsDoubleValues() {
                return true;
            }

            /**
             * Supports setting of a float value.
             */
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            default boolean supportsFloatValues() {
                return true;
            }

            /**
             * Supports setting of a integer value.
             */
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            default boolean supportsIntegerValues() {
                return true;
            }

            /**
             * Supports setting of a long value.
             */
            @FeatureDescriptor(name = FEATURE_LONG_VALUES)
            default boolean supportsLongValues() {
                return true;
            }

            /**
             * Supports setting of a {@code Map} value.  The assumption is that the {@code Map} can contain
             * arbitrary serializable values that may or may not be defined as a feature itself.
             */
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            default boolean supportsMapValues() {
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
            default boolean supportsMixedListValues() {
                return true;
            }

            /**
             * Supports setting of an array of boolean values.
             */
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            default boolean supportsBooleanArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of byte values.
             */
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            default boolean supportsByteArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of double values.
             */
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            default boolean supportsDoubleArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of float values.
             */
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            default boolean supportsFloatArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of integer values.
             */
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            default boolean supportsIntegerArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of string values.
             */
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            default boolean supportsStringArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of long values.
             */
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            default boolean supportsLongArrayValues() {
                return true;
            }

            /**
             * Supports setting of a Java serializable value.
             */
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            default boolean supportsSerializableValues() {
                return true;
            }

            /**
             * Supports setting of a string value.
             */
            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            default boolean supportsStringValues() {
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
            default boolean supportsUniformListValues() {
                return true;
            }
        }

        /**
         * A marker interface to identify any set of Features. There is no need to implement this interface.
         */
        interface FeatureSet {
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
    class Exceptions {

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

        public static IllegalArgumentException argumentCanNotBeNull(final String argument) {
            return new IllegalArgumentException(String.format("The provided argument can not be null: %s", argument));
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
    @interface OptIn {
        String SUITE_STRUCTURE_STANDARD = "org.apache.tinkerpop.gremlin.structure.StructureStandardSuite";
        String SUITE_STRUCTURE_INTEGRATE = "org.apache.tinkerpop.gremlin.structure.StructureIntegrateSuite";
        String SUITE_PROCESS_COMPUTER = "org.apache.tinkerpop.gremlin.process.ProcessComputerSuite";
        String SUITE_PROCESS_STANDARD = "org.apache.tinkerpop.gremlin.process.ProcessStandardSuite";
        String SUITE_PROCESS_LIMITED_COMPUTER = "org.apache.tinkerpop.gremlin.process.ProcessLimitedComputerSuite";
        String SUITE_PROCESS_LIMITED_STANDARD = "org.apache.tinkerpop.gremlin.process.ProcessLimitedStandardSuite";

        /**
         * The test suite class to opt in to.
         */
        String value();
    }

    /**
     * Holds a collection of {@link OptIn} enabling multiple {@link OptIn} to be applied to a
     * single suite.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    @interface OptIns {
        OptIn[] value();
    }

    /**
     * Defines a test in the suite that the implementer does not want to run.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Repeatable(OptOuts.class)
    @Inherited
    @interface OptOut {
        /**
         * The test class to opt out of. This may be set to a base class of a test as in the case of the Gremlin
         * process class of tests from which Gremlin flavors extend.  If the actual test class is an inner class
         * of then use a "$" as a separator between the outer class and inner class.
         */
        String test();

        /**
         * The specific name of the test method to opt out of or asterisk to opt out of all methods in a
         * {@link #test}.
         */
        String method();

        /**
         * The reason the implementation is opting out of this test.
         */
        String reason();

        /**
         * For parameterized tests specify the name of the test itself without its "square brackets".
         */
        String specific() default "";

        /**
         * The list of {@link GraphComputer} implementations by class name that a test should opt-out from using (i.e. other
         * graph computers not in this list will execute the test).  This setting should only be included when
         * the test is one that uses the {@code TraversalEngine.COMPUTER} - it will otherwise be ignored.  By
         * default, an empty array is assigned and it is thus assumed that all computers are excluded when an
         * {@code OptOut} annotation is used, therefore this value must be overridden to be more specific.
         */
        String[] computers() default {};

    }

    /**
     * Holds a collection of {@link OptOut} enabling multiple {@link OptOut} to be applied to a
     * single suite.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    @interface OptOuts {
        OptOut[] value();
    }
}
