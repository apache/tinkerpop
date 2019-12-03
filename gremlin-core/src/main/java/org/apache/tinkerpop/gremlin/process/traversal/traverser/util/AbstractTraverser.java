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
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractTraverser<T> implements Traverser<T>, Traverser.Admin<T> {

    protected T t;

    protected AbstractTraverser() {

    }

    public AbstractTraverser(final T t) {
        this.t = t;
    }

    /////////////

    @Override
    public void merge(final Admin<?> other) {
        throw new UnsupportedOperationException("This traverser does not support merging: " + this.getClass().getCanonicalName());
    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        try {
            final AbstractTraverser<R> clone = (AbstractTraverser<R>) super.clone();
            clone.t = r;
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Admin<T> split() {
        try {
            return (AbstractTraverser<T>) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void addLabels(final Set<String> labels) {

    }

    @Override
    public void keepLabels(final Set<String> labels) {

    }

    @Override
    public void dropLabels(final Set<String> labels) {

    }

    @Override
    public void dropPath() {

    }


    @Override
    public void set(final T t) {
        this.t = t;
    }


    @Override
    public void initialiseLoops(final String stepLabel, final String loopName) {

    }

    @Override
    public void incrLoops() {

    }

    @Override
    public void resetLoops() {

    }

    @Override
    public String getStepId() {
        throw new UnsupportedOperationException("This traverser does not support futures: " + this.getClass().getCanonicalName());
    }

    @Override
    public void setStepId(final String stepId) {

    }

    @Override
    public void setBulk(final long count) {

    }

    @Override
    public Admin<T> detach() {
        this.t = ReferenceFactory.detach(this.t);
        return this;
    }

    @Override
    public T attach(final Function<Attachable<T>, T> method) {
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        if (this.t instanceof Attachable && !(((Attachable) this.t).get() instanceof Path))
            this.t = ((Attachable<T>) this.t).attach(method);
        return this.t;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {

    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return EmptyTraversalSideEffects.instance();
        //throw new UnsupportedOperationException("This traverser does not support sideEffects: " + this.getClass().getCanonicalName());
    }

    @Override
    public T get() {
        return this.t;
    }

    @Override
    public <S> S sack() {
        throw new UnsupportedOperationException("This traverser does not support sacks: " + this.getClass().getCanonicalName());
    }

    @Override
    public <S> void sack(final S object) {

    }

    @Override
    public Path path() {
        return EmptyPath.instance();
    }

    @Override
    public int loops() {
        throw new UnsupportedOperationException("This traverser does not support loops: " + this.getClass().getCanonicalName());
    }

    @Override
    public int loops(final String loopName) {
        throw new UnsupportedOperationException("This traverser does not support named loops: " + this.getClass().getCanonicalName());
    }

    @Override
    public long bulk() {
        return 1l;
    }

    @Override
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public AbstractTraverser<T> clone() {
        try {
            return (AbstractTraverser<T>) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    ///////////

    @Override
    public int hashCode() {
        return Objects.hashCode(this.t);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof AbstractTraverser && Objects.equals(this.t, ((AbstractTraverser) object).t);
    }

    @Override
    public String toString() {
        return Objects.toString(this.t);
    }
}
