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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.structure.map;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantGVTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueStep;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Map;

public class MergeEdgeStepGV<S> extends GValueStep<MergeVertexStep, S, Edge> implements TraversalOptionParent<Merge, S, Map>, MergeElementStepStructure<S> {

    private Traversal.Admin<S, Map> merge;
    private Traversal.Admin<S, Map> create;
    private Traversal.Admin<S, Map> match;


    public MergeEdgeStepGV(final Traversal.Admin traversal, final boolean isStart) {
        super(traversal, new MergeVertexStep(traversal, isStart));
    }

    public MergeEdgeStepGV(final Traversal.Admin traversal, final boolean isStart, final Map merge) {
        super(traversal, new MergeVertexStep(traversal, isStart, merge));
    }

    public MergeEdgeStepGV(final Traversal.Admin traversal, final boolean isStart, final GValue<Map> merge) {
        super(traversal, new MergeVertexStep(traversal, isStart, merge.get()));
        this.merge = new ConstantGVTraversal<>(merge);

    }

    public MergeEdgeStepGV(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<S,Map> mergeTraversal) {
        super(traversal, new MergeVertexStep(traversal, isStart, mergeTraversal));
    }

    @Override
    public void addChildOption(final Merge token, final Traversal.Admin<S, Map> traversalOption) {
        if (token == Merge.onCreate) {
            if (traversalOption instanceof ConstantGVTraversal) {
                this.create = (Traversal.Admin<S, Map>) traversalOption;
                this.getConcreteStep().addChildOption(token, new ConstantTraversal(((ConstantGVTraversal) traversalOption).getEnd()));
            } else {
                this.getConcreteStep().addChildOption(token, traversalOption);
            }

        } else if (token == Merge.onMatch) {
            if (traversalOption instanceof ConstantGVTraversal) {
                this.match = (Traversal.Admin<S, Map>) traversalOption;
                this.getConcreteStep().addChildOption(token, new ConstantTraversal(((ConstantGVTraversal) traversalOption).getEnd()));
            } else {
                this.getConcreteStep().addChildOption(token, traversalOption);
            }
        } else {
            throw new UnsupportedOperationException(String.format("Option %s for Merge is not supported", token.name()));
        }
    }

    @Override
    public Traversal.Admin<S, Map> getMergeTraversal() {
        return merge;
    }

    @Override
    public Traversal.Admin<S, Map> getOnCreateTraversal() {
        return create;
    }

    @Override
    public Traversal.Admin<S, Map> getOnMatchTraversal() {
        return match;
    }
}
