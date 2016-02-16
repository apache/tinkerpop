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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PureTraversal<S, E> implements Serializable {

    private final Traversal.Admin<S, E> pureTraversal;
    private transient Traversal.Admin<S, E> compiledTraversal;

    public PureTraversal(final Traversal.Admin<S, E> pureTraversal) {
        this.pureTraversal = pureTraversal;
    }

    public Traversal.Admin<S, E> getPure() {
        return this.pureTraversal;
    }

    public Traversal.Admin<S, E> getCompiled() {
        if (null == this.compiledTraversal) {
            this.compiledTraversal = this.pureTraversal.clone();
            this.pureTraversal.getGraph().ifPresent(this.compiledTraversal::setGraph);
            if (!this.compiledTraversal.isLocked())
                this.compiledTraversal.applyStrategies();
        }
        return this.compiledTraversal;
    }

    public void storeState(final Configuration configuration, final String configurationKey) {
        try {
            VertexProgramHelper.serialize(this, configuration, configurationKey);   // the traversal can not be serialized (probably because of lambdas). As such, try direct reference.
        } catch (final IllegalArgumentException e) {
            configuration.setProperty(configurationKey, this);
        }
    }

    ////////////

    public static <S, E> void storeState(final Configuration configuration, final String configurationKey, final Traversal.Admin<S, E> traversal) {
        new PureTraversal<>(traversal).storeState(configuration, configurationKey);
    }

    public static <S, E> PureTraversal<S, E> loadState(final Configuration configuration, final String configurationKey, final Graph graph) {
        final Object configValue = configuration.getProperty(configurationKey);
        final PureTraversal<S, E> pureTraversal = (configValue instanceof String ? (PureTraversal<S, E>) VertexProgramHelper.deserialize(configuration, configurationKey) : ((PureTraversal<S, E>) configValue));
        pureTraversal.pureTraversal.setGraph(graph);
        return pureTraversal;
    }
}
