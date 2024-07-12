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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeEdgeStepPlaceholder<S> extends AbstractStep<S, Edge> implements MergeStepInterface<S, Edge, Map>, GValueHolder<S, Edge> {

    private static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label));

    public static void validateMapInput(final Map map, final boolean ignoreTokens) {
        MergeStep.validate(map, ignoreTokens, allowedTokens, "mergeV");
    }

    private Traversal.Admin<?,Map<Object,Object>> mergeTraversal;
    private Traversal.Admin<?,Map<Object,Object>> onCreateTraversal;
    private Traversal.Admin<?,Map<Object,Object>> onMatchTraversal;
    private final boolean isStart;

    public MergeEdgeStepPlaceholder(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, Collections.emptyMap());
    }

    public MergeEdgeStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final Map<Object, Object> merge) {
        this(traversal, isStart, new ConstantTraversal<>(merge));
    }

    public MergeEdgeStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final GValue<Map<Object, Object>> merge) {
        this(traversal, isStart, new GValueConstantTraversal<>(merge));
    }

    public MergeEdgeStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<?,Map<Object, Object>> mergeTraversal) {
        super(traversal);
        this.isStart = isStart;
        this.mergeTraversal = mergeTraversal;
    }

    @Override
    protected Traverser.Admin<Edge> processNextStart() throws NoSuchElementException {
        return null;
    }

    @Override
    public MergeEdgeStepPlaceholder<S> clone() {
        return (MergeEdgeStepPlaceholder<S>) super.clone();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return super.getRequirements();
    }

    /**
     * Fuse the mergeMap with any additional key/values from the onCreateTraversal. No overrides allowed.
     */
    @Override
    public Map<Object, Object> getMergeMap() {
        throw new UnsupportedOperationException("Cannot access merge map from step"); //TODO::
    }

    @Override
    public Map<Object, Object> getOnCreateMap() {
        throw new UnsupportedOperationException("Cannot access onCreate map from step"); //TODO::
    }

    @Override
    public Map<Object, Object> getOnMatchMap() {
        throw new UnsupportedOperationException("Cannot access onMatch map from step"); //TODO::
    }

    @Override
    public Traversal.Admin getMergeTraversal() {
        return mergeTraversal;
    }

    @Override
    public Traversal.Admin getOnCreateTraversal() {
        return onCreateTraversal;
    }

    @Override
    public Traversal.Admin getOnMatchTraversal() {
        return onMatchTraversal;
    }

    @Override
    public boolean isStart() {
        return isStart;
    }

    @Override
    public boolean isFirst() {
        return false; //TODO
    }

    @Override
    public void addChildOption(Merge token, Traversal.Admin traversalOption) {

    }

    @Override
    public boolean isUsingPartitionStrategy() {
        return false;
    }

    @Override
    public void setOnMatch(final Traversal.Admin<?,Map<Object, Object>> onMatchMap) {
        this.onMatchTraversal = onMatchMap;
        if (onMatchMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) onMatchMap).isParameterized()) {
            traversal.getGValueManager().track(((GValueConstantTraversal<?, Map<Object, Object>>) onMatchMap).getGValue());
        }
    }

    @Override
    public void setOnCreate(final Traversal.Admin<?,Map<Object, Object>> onCreateMap) {
        this.onCreateTraversal = onCreateMap;
        if (onCreateMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) onCreateMap).isParameterized()) {
            traversal.getGValueManager().track(((GValueConstantTraversal<?, Map<Object, Object>>) onCreateMap).getGValue());
        }
    }

    @Override
    public void setMerge(Traversal.Admin<?,Map<Object, Object>> mergeMap) {
        this.mergeTraversal = mergeMap;
        if (mergeMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) mergeMap).isParameterized()) {
            traversal.getGValueManager().track(((GValueConstantTraversal<?, Map<Object, Object>>) mergeMap).getGValue());
        }
    }

    @Override
    public Step<S, Edge> asConcreteStep() {
        MergeEdgeStep<S> step = new MergeEdgeStep<>(traversal, isStart,
                mergeTraversal instanceof GValueConstantTraversal ?
                        ((GValueConstantTraversal) mergeTraversal).getConstantTraversal() : mergeTraversal);
        step.setOnCreate(onCreateTraversal instanceof GValueConstantTraversal ?
                ((GValueConstantTraversal) onCreateTraversal).getConstantTraversal() : onCreateTraversal);
        step.setOnMatch(onMatchTraversal instanceof GValueConstantTraversal ?
                ((GValueConstantTraversal) onMatchTraversal).getConstantTraversal() : onMatchTraversal);
        return step;
    }

    @Override
    public boolean isParameterized() {
        return false; //TODO
    }

    @Override
    public void updateVariable(String name, Object value) {
        //TODO
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        return null; //TODO
    }
}
