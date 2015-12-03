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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTraverser<T> implements Traverser<T>, Traverser.Admin<T> {

    private static final EmptyTraverser INSTANCE = new EmptyTraverser();

    public static <R> EmptyTraverser<R> instance() {
        return INSTANCE;
    }

    private EmptyTraverser() {

    }

    @Override
    public void addLabels(final Set<String> labels) {

    }

    @Override
    public void set(final T t) {

    }

    @Override
    public void incrLoops(final String stepLabel) {

    }

    @Override
    public void resetLoops() {

    }

    @Override
    public String getStepId() {
        return HALT;
    }

    @Override
    public void setStepId(final String stepId) {

    }

    @Override
    public void setBulk(long count) {

    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        return INSTANCE;
    }

    @Override
    public Admin<T> split() {
        return this;
    }

    @Override
    public Admin<T> detach() {
        return this;
    }

    @Override
    public T attach(final Function<Attachable<T>, T> method) {
        return null;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {

    }

    @Override
    public T get() {
        return null;
    }

    @Override
    public <S> S sack() {
        return null;
    }

    @Override
    public <S> void sack(final S object) {

    }

    @Override
    public void merge(final Traverser.Admin<?> other) {

    }

    @Override
    public Path path() {
        return EmptyPath.instance();
    }

    @Override
    public int loops() {
        return 0;
    }

    @Override
    public long bulk() {
        return 0l;
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return null;
    }

    @Override
    public Set<String> getTags() {
        return Collections.emptySet();
    }

    @Override
    public int hashCode() {
        return 380473707;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyTraverser;
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public EmptyTraverser<T> clone() {
        return this;
    }
}
