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
package org.apache.tinkerpop.gremlin.groovy;

import groovy.grape.Grape;
import groovy.json.JsonBuilder;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.bulkdumping.BulkDumperVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedElement;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.5, replaced by {@link ImportCustomizer}
 */
@Deprecated
public abstract class AbstractImportCustomizerProvider implements ImportCustomizerProvider {
    protected static final String DOT_STAR = ".*";
    protected static final String EMPTY_STRING = "";
    protected static final String PERIOD = ".";

    protected final Set<String> extraImports = new HashSet<>();
    protected final Set<String> extraStaticImports = new HashSet<>();

    private static final Set<String> imports = new HashSet<>();
    private static final Set<String> staticImports = new HashSet<>();

    static {
        // graph structure
        imports.add(Graph.class.getPackage().getName() + DOT_STAR);
        imports.add(GraphFactory.class.getPackage().getName() + DOT_STAR);
        imports.add(DetachedElement.class.getPackage().getName() + DOT_STAR);
        imports.add(RemoteGraph.class.getPackage().getName() + DOT_STAR);
        imports.add(EmptyGraph.class.getPackage().getName() + DOT_STAR);
        staticImports.add(T.class.getCanonicalName() + DOT_STAR);
        staticImports.add(Direction.class.getCanonicalName() + DOT_STAR);
        staticImports.add(VertexProperty.Cardinality.class.getCanonicalName() + DOT_STAR);

        // graph process
        imports.add(Traversal.class.getPackage().getName() + DOT_STAR);
        imports.add(GraphComputer.class.getPackage().getName() + DOT_STAR);
        imports.add(GraphTraversal.class.getPackage().getName() + DOT_STAR);
        imports.add(ComputerTraversalEngine.class.getPackage().getName() + DOT_STAR);
        imports.add(PartitionStrategy.class.getPackage().getName() + DOT_STAR);       // decoration strategies
        imports.add(IdentityRemovalStrategy.class.getPackage().getName() + DOT_STAR); // optimization strategies
        imports.add(ProfileStrategy.class.getPackage().getName() + DOT_STAR);         // finalization strategies
        imports.add(ReadOnlyStrategy.class.getPackage().getName() + DOT_STAR);        // verification strategies
        imports.add(VertexProgramStrategy.class.getPackage().getName() + DOT_STAR);   // computer decoration strategies
        imports.add(GraphFilterStrategy.class.getPackage().getName() + DOT_STAR);     // computer optimization strategies
        imports.add(Event.class.getPackage().getName() + DOT_STAR);                   // eventing
        imports.add(GroovyTranslator.class.getPackage().getName() + DOT_STAR);        // groovy translator

        staticImports.add(P.class.getCanonicalName() + DOT_STAR);
        staticImports.add(Order.class.getCanonicalName() + DOT_STAR);
        staticImports.add(Column.class.getCanonicalName() + DOT_STAR);
        staticImports.add(Operator.class.getCanonicalName() + DOT_STAR);
        staticImports.add(Scope.class.getCanonicalName() + DOT_STAR);
        staticImports.add(Pop.class.getCanonicalName() + DOT_STAR);
        staticImports.add(__.class.getCanonicalName() + DOT_STAR);

        staticImports.add(SackFunctions.Barrier.class.getCanonicalName() + DOT_STAR);
        staticImports.add(TraversalOptionParent.Pick.class.getCanonicalName() + DOT_STAR);
        staticImports.add(GraphTraversalSource.class.getCanonicalName() + DOT_STAR);

        // utils
        imports.add(Gremlin.class.getPackage().getName() + DOT_STAR);
        imports.add(GremlinLoader.class.getPackage().getName() + DOT_STAR);
        imports.add(FunctionUtils.class.getPackage().getName() + DOT_STAR);
        imports.add(TraversalMetrics.class.getPackage().getName() + DOT_STAR);
        staticImports.add(TimeUtil.class.getCanonicalName() + DOT_STAR);
        staticImports.add(Computer.class.getCanonicalName() + DOT_STAR);

        // IO packages
        imports.add(GraphReader.class.getPackage().getName() + DOT_STAR);
        imports.add(GraphMLReader.class.getPackage().getName() + DOT_STAR);
        imports.add(GraphSONReader.class.getPackage().getName() + DOT_STAR);
        imports.add(GryoReader.class.getPackage().getName() + DOT_STAR);
        staticImports.add(IoCore.class.getCanonicalName() + DOT_STAR);

        // algorithms
        imports.add(PeerPressureVertexProgram.class.getPackage().getName() + DOT_STAR);
        imports.add(PageRankVertexProgram.class.getPackage().getName() + DOT_STAR);
        imports.add(TraversalVertexProgram.class.getPackage().getName() + DOT_STAR);
        imports.add(BulkLoaderVertexProgram.class.getPackage().getName() + DOT_STAR);
        imports.add(BulkDumperVertexProgram.class.getPackage().getName() + DOT_STAR);

        // groovy extras
        imports.add(Grape.class.getCanonicalName());
        imports.add(JsonBuilder.class.getPackage().getName() + DOT_STAR);

        // external
        imports.add(Configuration.class.getPackage().getName() + DOT_STAR);
    }

    @Override
    public CompilationCustomizer create() {
        final ImportCustomizer ic = new ImportCustomizer();

        processImports(ic, imports);
        processStaticImports(ic, staticImports);
        processImports(ic, extraImports);
        processStaticImports(ic, extraStaticImports);

        return ic;
    }

    @Override
    public Set<String> getImports() {
        return imports;
    }

    @Override
    public Set<String> getStaticImports() {
        return staticImports;
    }

    @Override
    public Set<String> getExtraImports() {
        return extraImports;
    }

    @Override
    public Set<String> getExtraStaticImports() {
        return extraStaticImports;
    }

    public Set<String> getAllImports() {
        final Set<String> allImports = new HashSet<>();
        allImports.addAll(imports);
        allImports.addAll(staticImports);
        allImports.addAll(extraImports);
        allImports.addAll(extraStaticImports);

        return allImports;
    }

    protected static void processStaticImports(final ImportCustomizer ic, final Set<String> staticImports) {
        for (final String staticImport : staticImports) {
            if (staticImport.endsWith(DOT_STAR)) {
                ic.addStaticStars(staticImport.replace(DOT_STAR, EMPTY_STRING));
            } else {
                final int place = staticImport.lastIndexOf(PERIOD);
                ic.addStaticImport(staticImport.substring(0, place), staticImport.substring(place + 1));
            }
        }
    }

    protected static void processImports(final ImportCustomizer ic, final Set<String> imports) {
        for (final String imp : imports) {
            if (imp.endsWith(DOT_STAR)) {
                ic.addStarImports(imp.replace(DOT_STAR, EMPTY_STRING));
            } else {
                ic.addImports(imp);
            }
        }
    }
}
