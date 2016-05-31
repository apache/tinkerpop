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

package org.apache.tinkerpop.gremlin.process.variant

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.process.variant.python.GremlinPythonGenerator
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex

import javax.script.ScriptEngine
import javax.script.ScriptEngineManager

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VariantGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    protected String variantString;
    protected VariantConverter variantConverter;

    public VariantGraphTraversal(
            final Graph graph, final String start, final VariantConverter variantConverter) {
        super(graph);
        this.variantConverter = variantConverter;
        this.variantString = start;
    }


    public GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        this.variantString = this.variantString + this.variantConverter.step("bothE", edgeLabels);
        return this;
    }

    public GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        this.variantString = this.variantString + this.variantConverter.step("outE", edgeLabels);
        return this;
    }

    public GraphTraversal<S, Vertex> inV() {
        this.variantString = this.variantString + this.variantConverter.step("inV");
        return this;
    }

    public GraphTraversal<S, Vertex> otherV() {
        this.variantString = this.variantString + this.variantConverter.step("otherV");
        return this;
    }

    public GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        this.variantString = this.variantString + this.variantConverter.step("both", edgeLabels);
        return this;
    }

    public GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        this.variantString = this.variantString + this.variantConverter.step("out", edgeLabels);
        return this;
    }

    public GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        this.variantString = this.variantString + this.variantConverter.step("inE", edgeLabels);
        return this;
    }

    public GraphTraversal<S, E> values(final String... propertyNames) {
        this.variantString = this.variantString + this.variantConverter.step("values", propertyNames);
        return this;
    }


    @Override
    public void applyStrategies() {
        if (!this.locked) {
            try {
                GremlinPythonGenerator.create("/tmp");
                ScriptEngine engine = new ScriptEngineManager().getEngineByName("jython");
                engine.eval("execfile(\"/tmp/gremlin-python-3.2.1-SNAPSHOT.py\")");
                engine.eval("g = PythonGraphTraversalSource(\"g\")");
                System.out.println(this.variantString + "!!!!!!!!");
                final ScriptTraversal<?, ?> traversal = new ScriptTraversal(new GraphTraversalSource(this.getGraph().get(), this.getStrategies()), "gremlin-groovy", engine.eval(this.variantString).toString());
                traversal.applyStrategies();
                traversal.getSideEffects().mergeInto(this.sideEffects);
                traversal.getSteps().forEach { step -> this.addStep(step) };
            } catch (final Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        super.applyStrategies();
    }


}