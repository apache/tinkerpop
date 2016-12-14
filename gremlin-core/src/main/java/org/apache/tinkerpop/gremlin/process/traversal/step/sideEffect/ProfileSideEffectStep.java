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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Pushing;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.DefaultTraversalMetricsSupplier;

import java.util.function.Supplier;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileSideEffectStep<S> extends SideEffectStep<S> implements SideEffectCapable<DefaultTraversalMetrics, DefaultTraversalMetrics>, GraphComputing, Pushing {
    public static final String DEFAULT_METRICS_KEY = Graph.Hidden.hide("metrics");

    private String sideEffectKey;
    private boolean pushBased = false;

    public ProfileSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, (Supplier) DefaultTraversalMetricsSupplier.instance(), Operator.assign);
    }

    @Override
    protected void sideEffect(Traverser.Admin<S> traverser) {
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public Traverser.Admin<S> next() {
        Traverser.Admin<S> start = null;
        try {
            start = super.next();
            return start;
        } finally {
            if (!this.pushBased && start == null) {
                ((DefaultTraversalMetrics) this.getTraversal().getSideEffects().get(this.sideEffectKey)).setMetrics(this.getTraversal(), false);
            }
        }
    }

    @Override
    public boolean hasNext() {
        boolean start = super.hasNext();
        if (!this.pushBased && !start) {
            ((DefaultTraversalMetrics) this.getTraversal().getSideEffects().get(this.sideEffectKey)).setMetrics(this.getTraversal(), false);
        }
        return start;
    }

    @Override
    public DefaultTraversalMetrics generateFinalResult(final DefaultTraversalMetrics tm) {
        if (this.pushBased)
            tm.setMetrics(this.getTraversal(), true);
        return tm;
    }

    @Override
    public void onGraphComputer() {
        this.setPushBased(true);
    }

    @Override
    public void setPushBased(boolean pushBased) {
        this.pushBased = pushBased;
    }
}
