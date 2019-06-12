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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code EdgeLabelVerificationStrategy} does not allow edge traversal steps to have no label specified.
 * Providing one or more labels is considered to be a best practice, however, TinkerPop will not force the specification
 * of edge labels; instead, providers or users will have to enable this strategy explicitly.
 * <p/>
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @example <pre>
 * __.outE()           // throws an IllegalStateException
 * __.out()            // throws an IllegalStateException
 * __.bothE()          // throws an IllegalStateException
 * __.to(OUT)          // throws an IllegalStateException
 * __.toE(IN)          // throws an IllegalStateException
 * </pre>
 */
public final class EdgeLabelVerificationStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy>
        implements TraversalStrategy.VerificationStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeLabelVerificationStrategy.class);

    private static final String THROW_EXCEPTION = "throwException";
    private static final String LOG_WARNING = "logWarning";

    private final boolean throwException;
    private final boolean logWarning;

    private EdgeLabelVerificationStrategy(final boolean throwException, final boolean logWarning) {
        this.throwException = throwException;
        this.logWarning = logWarning;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof VertexStep && ((VertexStep) step).getEdgeLabels().length == 0) {
                final String msg = String.format(
                        "The provided traversal contains a vertex step without any specified edge label: %s\nAlways " +
                                "specify edge labels which restrict traversal paths ensuring optimal performance.", step);
                if (logWarning) {
                    LOGGER.warn(msg);
                }
                if (throwException) {
                    throw new VerificationException(msg, traversal);
                }
            }
        }
    }

    public static EdgeLabelVerificationStrategy create(final Configuration configuration) {
        return new EdgeLabelVerificationStrategy(
                configuration.getBoolean(THROW_EXCEPTION, false),
                configuration.getBoolean(LOG_WARNING, false));
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> m = new HashMap<>(2);
        m.put(THROW_EXCEPTION, this.throwException);
        m.put(LOG_WARNING, this.logWarning);
        return new MapConfiguration(m);
    }

    public static EdgeLabelVerificationStrategy.Builder build() {
        return new EdgeLabelVerificationStrategy.Builder();
    }

    public final static class Builder {

        private boolean throwException;
        private boolean logWarning;

        private Builder() {
        }

        public EdgeLabelVerificationStrategy.Builder throwException() {
            return this.throwException(true);
        }

        public EdgeLabelVerificationStrategy.Builder throwException(final boolean throwException) {
            this.throwException = throwException;
            return this;
        }

        public EdgeLabelVerificationStrategy.Builder logWarning() {
            return this.logWarning(true);
        }

        public EdgeLabelVerificationStrategy.Builder logWarning(final boolean logWarning) {
            this.logWarning = logWarning;
            return this;
        }

        public EdgeLabelVerificationStrategy create() {
            return new EdgeLabelVerificationStrategy(this.throwException, this.logWarning);
        }
    }
}
