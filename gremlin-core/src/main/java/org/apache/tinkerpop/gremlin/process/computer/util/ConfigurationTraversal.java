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
package org.apache.tinkerpop.gremlin.process.computer.util;


import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.0, replaced by {@link org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal}.
 */
@Deprecated
public final class ConfigurationTraversal<S, E> implements Supplier<Traversal.Admin<S, E>> {

    private Function<Graph, Traversal.Admin<S, E>> traversalFunction;
    private String configKey;
    private Graph graph;

    public Traversal.Admin<S, E> get() {
        return this.traversalFunction.apply(this.graph);
    }

    private ConfigurationTraversal() {

    }

    public void storeState(final Configuration configuration) {
        try {
            VertexProgramHelper.serialize(this.traversalFunction, configuration, this.configKey);   // the traversal can not be serialized (probably because of lambdas). As such, try direct reference.
        } catch (final IllegalArgumentException e) {
            configuration.setProperty(this.configKey, this.traversalFunction);
        }
    }

    public static <S, E> ConfigurationTraversal<S, E> storeState(final Function<Graph, Traversal.Admin<S, E>> traversalFunction, final Configuration configuration, final String configKey) {
        final ConfigurationTraversal<S, E> configurationTraversal = new ConfigurationTraversal<>();
        configurationTraversal.configKey = configKey;
        configurationTraversal.traversalFunction = traversalFunction;
        configurationTraversal.storeState(configuration);
        return configurationTraversal;
    }

    public static <S, E> ConfigurationTraversal<S, E> loadState(final Graph graph, final Configuration configuration, final String configKey) {
        final ConfigurationTraversal<S, E> configurationTraversal = new ConfigurationTraversal<>();
        configurationTraversal.graph = graph;
        configurationTraversal.configKey = configKey;
        final Object configValue = configuration.getProperty(configKey);
        configurationTraversal.traversalFunction = configValue instanceof String ? VertexProgramHelper.deserialize(configuration, configKey) : (Function<Graph, Traversal.Admin<S, E>>) configValue;
        return configurationTraversal;
    }
}