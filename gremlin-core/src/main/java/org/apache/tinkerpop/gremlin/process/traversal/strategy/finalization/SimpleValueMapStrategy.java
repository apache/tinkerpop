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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.PropertyType;

/**
 * SimpleValueMapStrategy automatically adds a {@code .by(unfold())} to every {@code valueMap()} step that has no
 * by-modulator specified. This is useful especially for providers who do not support multi-valued properties.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class SimpleValueMapStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy>
        implements TraversalStrategy.FinalizationStrategy {

    private final static SimpleValueMapStrategy INSTANCE = new SimpleValueMapStrategy();

    private SimpleValueMapStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof PropertyMapStep) {
                final PropertyMapStep pms = (PropertyMapStep) step;
                if (pms.getReturnType() == PropertyType.VALUE && pms.getLocalChildren().isEmpty()) {
                    //noinspection unchecked
                    pms.modulateBy(__.unfold().asAdmin());
                }
            }
        }
    }

    public static SimpleValueMapStrategy instance() {
        return INSTANCE;
    }
}