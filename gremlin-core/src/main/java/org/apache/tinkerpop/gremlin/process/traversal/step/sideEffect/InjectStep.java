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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InjectStep<S> extends StartStep<S> {

    private final GValue<S>[] injections;

    @SafeVarargs
    public InjectStep(final Traversal.Admin traversal, final S... injections) {
        super(traversal);
        this.injections = GValue.ensureGValues(injections);
        this.start = new ArrayIterator<>(GValue.resolveToValues(this.injections));
    }

    @Override
    public InjectStep<S> clone() {
        final InjectStep<S> clone = (InjectStep<S>) super.clone();
        clone.start = new ArrayIterator<>(GValue.resolveToValues(clone.injections));
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.start = new ArrayIterator<>(GValue.resolveToValues(this.injections));
    }

    /**
     * Get the injections of the step.
     */
    public GValue<S>[] getInjectionsGValue() {
        return this.injections;
    }

    /**
     * Gets the injections of the step but unwraps the {@link GValue}.
     */
    public S[] getInjections() {
        return (S[]) GValue.resolveToValues(this.injections);
    }
}
