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
package org.apache.tinkerpop.gremlin.hadoop.groovy.plugin;

import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteException;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.4, replaced by {@link org.apache.tinkerpop.gremlin.hadoop.jsr223.HadoopRemoteAcceptor}.
 */
@Deprecated
public final class HadoopRemoteAcceptor implements RemoteAcceptor {

    private static final String USE_SUGAR = "useSugar";
    private static final String USE_TRAVERSAL_SOURCE = "useTraversalSource";
    private static final String SPACE = " ";

    private HadoopGraph hadoopGraph;
    private Groovysh shell;
    private boolean useSugar = false;
    private TraversalSource traversalSource;

    public HadoopRemoteAcceptor(final Groovysh shell) {
        this.shell = shell;
    }

    @Override
    public Object connect(final List<String> args) throws RemoteException {
        if (args.size() != 1 && args.size() != 2) {
            throw new IllegalArgumentException("Usage: :remote connect " + HadoopGremlinPlugin.NAME + " <variable name of graph> <optional variable name of traversal source>");
        }
        this.hadoopGraph = (HadoopGraph) this.shell.getInterp().getContext().getVariable(args.get(0));
        if (args.size() == 2)
            this.traversalSource = ((TraversalSource) this.shell.getInterp().getContext().getVariable(args.get(1)));
        else
            this.traversalSource = this.hadoopGraph.traversal();
        ///
        final HashMap<String, Object> configuration = new HashMap<>();
        configuration.put(USE_SUGAR, this.useSugar);
        configuration.put(USE_TRAVERSAL_SOURCE, this.traversalSource);
        return Collections.unmodifiableMap(configuration);
    }

    @Override
    public Object configure(final List<String> args) throws RemoteException {
        for (int i = 0; i < args.size(); i = i + 2) {
            if (args.get(i).equals(USE_SUGAR))
                this.useSugar = Boolean.valueOf(args.get(i + 1));
            else if (args.get(i).equals(USE_TRAVERSAL_SOURCE)) {
                this.traversalSource = ((TraversalSource) this.shell.getInterp().getContext().getVariable(args.get(i + 1)));
            } else
                throw new IllegalArgumentException("The provided configuration is unknown: " + args.get(i) + ":" + args.get(i + 1));
        }
        ///
        final HashMap<String, Object> configuration = new HashMap<>();
        configuration.put(USE_SUGAR, this.useSugar);
        configuration.put(USE_TRAVERSAL_SOURCE, this.traversalSource);
        return Collections.unmodifiableMap(configuration);
    }

    @Override
    public Object submit(final List<String> args) throws RemoteException {
        try {
            String script = RemoteAcceptor.getScript(String.join(SPACE, args), this.shell);
            if (this.useSugar)
                script = SugarLoader.class.getCanonicalName() + ".load()\n" + script;
            final TraversalVertexProgram program = TraversalVertexProgram.build().traversal(this.traversalSource, "gremlin-groovy", script).create(this.hadoopGraph);
            final ComputerResult computerResult = VertexProgramStrategy.getComputer(this.traversalSource.getStrategies()).get().apply(this.hadoopGraph).program(program).submit().get();
            this.shell.getInterp().getContext().setVariable(RESULT, computerResult);
            ///
            final Traversal.Admin<ComputerResult, ?> traversal = new DefaultTraversal<>(computerResult.graph());
            traversal.addStep(new ComputerResultStep<>(traversal));
            traversal.addStart(traversal.getTraverserGenerator().generate(computerResult, EmptyStep.instance(), 1l));
            return traversal;
        } catch (final Exception e) {
            throw new RemoteException(e);
        }
    }

    @Override
    public boolean allowRemoteConsole() {
        return true;
    }

    @Override
    public void close() throws IOException {
        this.hadoopGraph.close();
    }
}
