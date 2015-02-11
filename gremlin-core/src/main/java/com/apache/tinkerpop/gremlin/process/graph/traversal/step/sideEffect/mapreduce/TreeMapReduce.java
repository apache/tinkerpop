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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce;

import com.apache.tinkerpop.gremlin.process.computer.KeyValue;
import com.apache.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import com.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.TreeStep;
import com.apache.tinkerpop.gremlin.process.graph.util.Tree;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeMapReduce extends StaticMapReduce<Object, Tree, Object, Tree, Tree> {

    public static final String TREE_STEP_SIDE_EFFECT_KEY = "gremlin.treeStep.sideEffectKey";

    private String sideEffectKey;

    private TreeMapReduce() {

    }

    public TreeMapReduce(final TreeStep step) {
        this.sideEffectKey = step.getSideEffectKey();
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(TREE_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(TREE_STEP_SIDE_EFFECT_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Object, Tree> emitter) {
        VertexTraversalSideEffects.of(vertex).<Tree<?>>ifPresent(this.sideEffectKey, tree -> tree.splitParents().forEach(branches -> emitter.emit(branches.keySet().iterator().next(), branches)));
    }

    @Override
    public Tree generateFinalResult(final Iterator<KeyValue<Object, Tree>> keyValues) {
        final Tree result = new Tree();
        keyValues.forEachRemaining(keyValue -> result.addTree(keyValue.getValue()));
        return result;
    }

    @Override
    public String getMemoryKey() {
        return this.sideEffectKey;
    }
}
