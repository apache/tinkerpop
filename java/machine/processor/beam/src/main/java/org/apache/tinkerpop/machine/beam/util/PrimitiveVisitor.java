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
package org.apache.tinkerpop.machine.beam.util;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.tinkerpop.machine.beam.OutputFn;
import org.apache.tinkerpop.machine.beam.RepeatDeadEndFn;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PrimitiveVisitor implements Pipeline.PipelineVisitor {

    private final List<PTransform> primitives = new ArrayList<>();

    @Override
    public void enterPipeline(final Pipeline p) {

    }

    @Override
    public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(final TransformHierarchy.Node node) {

    }

    @Override
    public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
        if (!node.getTransform().toString().startsWith("Read") &&
                !node.getTransform().toString().contains(OutputFn.class.getSimpleName()) &&
                !node.getTransform().toString().contains(RepeatDeadEndFn.class.getSimpleName()) &&
                !node.getTransform().toString().startsWith("Flatten"))
            this.primitives.add(node.getTransform());
    }

    @Override
    public void visitValue(final PValue value, final TransformHierarchy.Node producer) {
    }

    @Override
    public void leavePipeline(final Pipeline pipeline) {

    }

    @Override
    public String toString() {
        return this.primitives.toString();
    }
}
