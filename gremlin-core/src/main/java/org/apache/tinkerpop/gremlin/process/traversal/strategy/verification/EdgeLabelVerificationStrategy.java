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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStepContract;

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
public final class EdgeLabelVerificationStrategy extends AbstractWarningVerificationStrategy {

    private EdgeLabelVerificationStrategy(final Builder builder) {
        super(builder);
    }

    @Override
    void verify(final Traversal.Admin<?, ?> traversal) throws VerificationException {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof VertexStepContract && ((VertexStepContract) step).getEdgeLabels().length == 0) {
                final String msg = String.format(
                        "The provided traversal contains a vertex step without any specified edge label: %s\nAlways " +
                                "specify edge labels which restrict traversal paths ensuring optimal performance.", step);
                throw new VerificationException(msg, traversal);
            }
        }
    }

    public static EdgeLabelVerificationStrategy create(final Configuration configuration) {
        return build().throwException(configuration.getBoolean(THROW_EXCEPTION, false))
                        .logWarning(configuration.getBoolean(LOG_WARNING, false)).create();
    }

    public static EdgeLabelVerificationStrategy.Builder build() {
        return new EdgeLabelVerificationStrategy.Builder();
    }

    public final static class Builder extends AbstractWarningVerificationStrategy.Builder<EdgeLabelVerificationStrategy, Builder> {
        private Builder() {}

        @Override
        public EdgeLabelVerificationStrategy create() {
            return new EdgeLabelVerificationStrategy(this);
        }
    }
}
