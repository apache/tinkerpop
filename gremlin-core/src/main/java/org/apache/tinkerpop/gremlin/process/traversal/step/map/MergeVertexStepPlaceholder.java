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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeVertexStepPlaceholder<S> extends AbstractMergeElementStepPlaceholder<S, Vertex> {

    private static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label));

    public static void validateMapInput(final Map map, final boolean ignoreTokens) {
        MergeElementStep.validate(map, ignoreTokens, allowedTokens, "mergeV");
    }

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal<>());
    }

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final Map<Object, Object> merge) {
        this(traversal, isStart, new ConstantTraversal<>(merge));
        validateMapInput(merge, false);
    }

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final GValue<Map<Object, Object>> merge) {
        this(traversal, isStart, new GValueConstantTraversal<>(merge));
        validateMapInput(merge.get(), false);
    }

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<?,Map<Object, Object>> mergeTraversal) {
        super(traversal, mergeTraversal, isStart);
        if (mergeTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).isParameterized()) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).getGValue());
        }
    }

    @Override
    public MergeVertexStep<S> asConcreteStep() {
        MergeVertexStep<S> step = new MergeVertexStep<>(traversal, isStart,
                mergeTraversal instanceof GValueConstantTraversal ?
                        ((GValueConstantTraversal) mergeTraversal).getConstantTraversal() : mergeTraversal);
        step.setOnCreate(onCreateTraversal instanceof GValueConstantTraversal ?
                ((GValueConstantTraversal) onCreateTraversal).getConstantTraversal() : onCreateTraversal);
        step.setOnMatch(onMatchTraversal instanceof GValueConstantTraversal ?
                ((GValueConstantTraversal) onMatchTraversal).getConstantTraversal() : onMatchTraversal);
        TraversalHelper.copyLabels(this, step, false);

        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (final Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value);
            }
        }

        return step;
    }

    @Override
    public void addChildOption(Merge token, Traversal.Admin traversalOption) {
        if (token == Merge.onCreate) {
            setOnCreate(traversalOption);
        } else if (token == Merge.onMatch) {
            setOnMatch(traversalOption);
        } else {
            throw new UnsupportedOperationException(String.format("Option %s for Merge is not supported", token.name()));
        }
    }

}
