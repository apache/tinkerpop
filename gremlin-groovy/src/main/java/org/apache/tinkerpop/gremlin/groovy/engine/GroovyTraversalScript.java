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
package org.apache.tinkerpop.gremlin.groovy.engine;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalScript;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTraversalScript<S, E> implements TraversalScript<S, E> {

    private static final String ENGINE_NAME = new GremlinGroovyScriptEngineFactory().getEngineName();

    protected String openGraphScript;
    protected String traversalScript;
    protected String withSugarScript;

    private GraphComputer graphComputer;

    private GroovyTraversalScript(final String traversalScript) {
        this.traversalScript = traversalScript.concat("\n");
    }

    public static <S, E> GroovyTraversalScript<S, E> of(final String traversalScript) {
        return new GroovyTraversalScript<>(traversalScript);
    }

    @Override
    public GroovyTraversalScript<S, E> over(final Graph graph) {
        final Configuration configuration = graph.configuration();
        final StringBuilder configurationMap = new StringBuilder("g = GraphFactory.open([");
        configuration.getKeys().forEachRemaining(key -> configurationMap.append("'").append(key).append("':'").append(configuration.getProperty(key)).append("',"));
        configurationMap.deleteCharAt(configurationMap.length() - 1).append("])\n");
        this.openGraphScript = configurationMap.toString();
        return this;
    }

    @Override
    public GroovyTraversalScript<S, E> using(final GraphComputer graphComputer) {
        this.graphComputer = graphComputer;
        return this;
    }


    public GroovyTraversalScript<S, E> withSugar() {
        this.withSugarScript = SugarLoader.class.getCanonicalName() + ".load()\n";
        return this;
    }

    @Override
    public Future<ComputerResult> result() {
        return this.graphComputer.program(this.program()).submit();
    }

    @Override
    public Future<Traversal<S, E>> traversal() {
        return CompletableFuture.<Traversal<S, E>>supplyAsync(() -> {
            try {
                final TraversalVertexProgram vertexProgram = this.program();
                final ComputerResult result = this.graphComputer.program(vertexProgram).submit().get();
                final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(result.graph().getClass());
                return traversal.addStep(new ComputerResultStep<>(traversal, result, vertexProgram, true));
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
    }

    @Override
    public TraversalVertexProgram program() {
        return TraversalVertexProgram.build().traversal(ENGINE_NAME, this.makeFullScript()).create();
    }

    private String makeFullScript() {
        final StringBuilder builder = new StringBuilder();
        if (null != this.withSugarScript)
            builder.append(this.withSugarScript);
        if (null != this.openGraphScript)
            builder.append(this.openGraphScript);
        if (null != this.traversalScript)
            builder.append(this.traversalScript);
        return builder.toString();
    }
}
