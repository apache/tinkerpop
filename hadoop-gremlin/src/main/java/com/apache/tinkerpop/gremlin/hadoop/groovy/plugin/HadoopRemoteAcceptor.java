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
package com.apache.tinkerpop.gremlin.hadoop.groovy.plugin;

import com.apache.tinkerpop.gremlin.groovy.engine.GroovyTraversalScript;
import com.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.apache.tinkerpop.gremlin.groovy.plugin.RemoteException;
import com.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import com.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.apache.tinkerpop.gremlin.process.graph.traversal.DefaultGraphTraversal;
import com.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import com.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopRemoteAcceptor implements RemoteAcceptor {

    private static final String USE_SUGAR = "useSugar";
    private static final String SPACE = " ";

    private HadoopGraph hadoopGraph;
    private Groovysh shell;
    private boolean useSugarPlugin = false;
    private String graphVariable = "g";

    public HadoopRemoteAcceptor(final Groovysh shell) {
        this.shell = shell;
    }

    @Override
    public Object connect(final List<String> args) throws RemoteException {
        if (args.size() == 0) {
            this.hadoopGraph = HadoopGraph.open(new BaseConfiguration());
            this.shell.getInterp().getContext().setProperty("g", this.hadoopGraph);
        }
        if (args.size() == 1) {
            try {
                final FileConfiguration configuration = new PropertiesConfiguration();
                configuration.load(new File(args.get(0)));
                this.hadoopGraph = HadoopGraph.open(configuration);
                this.shell.getInterp().getContext().setProperty("g", this.hadoopGraph);
            } catch (final Exception e) {
                throw new RemoteException(e.getMessage(), e);
            }
        } else if (args.size() == 2) {
            try {
                final FileConfiguration configuration = new PropertiesConfiguration();
                configuration.load(new File(args.get(0)));
                this.hadoopGraph = HadoopGraph.open(configuration);
                this.graphVariable = args.get(1);
                this.shell.getInterp().getContext().setProperty(args.get(1), this.hadoopGraph);
            } catch (final Exception e) {
                throw new RemoteException(e.getMessage(), e);
            }
        }

        return this.hadoopGraph;
    }

    @Override
    public Object configure(final List<String> args) throws RemoteException {
        for (int i = 0; i < args.size(); i = i + 2) {
            if (args.get(i).equals(USE_SUGAR))
                this.useSugarPlugin = Boolean.valueOf(args.get(i + 1));
            else {
                this.hadoopGraph.configuration().setProperty(args.get(i), args.get(i + 1));
            }
        }
        return this.hadoopGraph;
    }

    @Override
    public Object submit(final List<String> args) throws RemoteException {
        try {
            final GroovyTraversalScript<?, ?> traversal = GroovyTraversalScript.of(RemoteAcceptor.getScript(String.join(SPACE, args), this.shell)).over(this.hadoopGraph).using(this.hadoopGraph.compute());
            if (this.useSugarPlugin)
                traversal.withSugar();
            final TraversalVertexProgram vertexProgram = traversal.program();
            final ComputerResult computerResult = traversal.result().get();
            this.shell.getInterp().getContext().setProperty(RESULT, computerResult);

            final GraphTraversal.Admin traversal2 = new DefaultGraphTraversal<>(Graph.class);
            traversal2.addStep(new ComputerResultStep<>(traversal2, computerResult, vertexProgram, false));
            traversal2.range(0, 19);
            return traversal2;
        } catch (Exception e) {
            throw new RemoteException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.hadoopGraph.close();
    }

    public HadoopGraph getGraph() {
        return this.hadoopGraph;
    }

    public String toString() {
        return "HadoopRemoteAcceptor[" + this.hadoopGraph + "]";
    }
}
