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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class GValueStep<C extends Step, S, E> extends AbstractStep<S,E> {

    protected final C concreteStep;

    public GValueStep(final Traversal.Admin traversal, final C concreteStep) {
        super(traversal);
        this.concreteStep = concreteStep;
    }

    public C getConcreteStep() {
        return concreteStep;
    }

    @Override
    public void addLabel(String label) {
        this.concreteStep.addLabel(label);
    }

    @Override
    public void removeLabel(String label) {
        this.concreteStep.removeLabel(label);
    }

    @Override
    public void clearLabels() {
        this.concreteStep.clearLabels();
    }

    @Override
    public Set<String> getLabels() {
        return this.concreteStep.getLabels();
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException("GValueStep is a placeholder step and should not be executed");
    }

    @Override
    public String toString() {
        return Objects.toString(concreteStep);
    }

    public static class MergeOptionStep<S,E> extends GValueStep<EmptyStep, S, E> {
        private final Merge merge;
        private final GValue gvalue;

        public MergeOptionStep(final Traversal.Admin traversal, final Merge merge, final GValue<Map<Object, Object>> gvalue) {
            super(traversal, EmptyStep.instance());
            this.merge = merge;
            this.gvalue = gvalue;
        }

        public Merge getMerge() {
            return merge;
        }

        public GValue getGvalue() {
            return gvalue;
        }
    }

}
