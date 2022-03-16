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
package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

/**
 * Similar to the EmptyTraverser, except the DummyTraverser can split. When it splits it generates an entirely
 * new traverser using its supplied TraverserGenerator, leaving no trace of the initial DummyTraverser in the path.
 * Useful for seeding nested traversals inside of start steps.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public final class DummyTraverser<T> extends EmptyTraverser<T> {

    private final TraverserGenerator generator;
    private TraversalSideEffects sideEffects;

    public DummyTraverser(final TraverserGenerator generator) {
        this.generator = generator;
    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        return generator.generate(r, (Step) step, 1L);
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return sideEffects;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof DummyTraverser;
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public DummyTraverser<T> clone() {
        return this;
    }
    
}
