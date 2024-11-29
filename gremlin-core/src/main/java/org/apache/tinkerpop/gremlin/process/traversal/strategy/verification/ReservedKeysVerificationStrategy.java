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
import org.apache.tinkerpop.gremlin.process.traversal.step.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This verification strategy detects property keys that should not be used by the traversal. A term may be reserved
 * by a particular graph implementation or as a convention given best practices.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 *  * @example <pre>
 *  * __.addV("person").property("id", 123)           // throws an IllegalStateException
 *  * __.addE("knows").property("label", "green")     // throws an IllegalStateException
 *  * </pre>
 */
public class ReservedKeysVerificationStrategy extends AbstractWarningVerificationStrategy {

    public static final String KEYS = "keys";
    private static final Set<String> DEFAULT_RESERVED_KEYS = new LinkedHashSet<>(Arrays.asList("id", "label"));
    private final Set<String> reservedKeys;

    private ReservedKeysVerificationStrategy(final Builder builder) {
        super(builder);
        this.reservedKeys = builder.reservedKeys;
    }

    @Override
    void verify(final Traversal.Admin<?, ?> traversal) throws VerificationException {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof AddVertexStep || step instanceof AddVertexStartStep ||
                step instanceof AddEdgeStartStep || step instanceof AddEdgeStep ||
                step instanceof AddPropertyStep) {
                final Parameterizing propertySettingStep = (Parameterizing) step;
                final Parameters params = propertySettingStep.getParameters();
                for (String key : reservedKeys) {
                    if (params.contains(key)) {
                        final String msg = String.format(
                                "The provided traversal contains a %s that is setting a property key to a reserved" +
                                        " word: %s", propertySettingStep.getClass().getSimpleName(), key);
                        throw new VerificationException(msg, traversal);
                    }
                }
            }
        }
    }

    public static ReservedKeysVerificationStrategy create(final Configuration configuration) {
        return build()
                .reservedKeys(configuration.getList(KEYS, new ArrayList<>(DEFAULT_RESERVED_KEYS)).
                        stream().map(Object::toString).collect(Collectors.toCollection(LinkedHashSet::new)))
                .throwException(configuration.getBoolean(THROW_EXCEPTION, false))
                .logWarning(configuration.getBoolean(LOG_WARNING, false)).create();
    }

    @Override
    public Configuration getConfiguration() {
        final Configuration c = super.getConfiguration();
        c.setProperty(KEYS, this.reservedKeys);
        return c;
    }

    public static ReservedKeysVerificationStrategy.Builder build() {
        return new ReservedKeysVerificationStrategy.Builder();
    }

    public final static class Builder extends AbstractWarningVerificationStrategy.Builder<ReservedKeysVerificationStrategy, Builder> {
        private Set<String> reservedKeys = DEFAULT_RESERVED_KEYS;

        private Builder() {}

        public Builder reservedKeys(final Set<String> keys) {
            this.reservedKeys = new LinkedHashSet<>(keys);
            return this;
        }

        @Override
        public ReservedKeysVerificationStrategy create() {
            return new ReservedKeysVerificationStrategy(this);
        }
    }
}
