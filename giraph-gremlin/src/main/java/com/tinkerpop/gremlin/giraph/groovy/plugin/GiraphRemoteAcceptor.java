package com.tinkerpop.gremlin.giraph.groovy.plugin;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.groovy.engine.function.GSSupplier;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.strategy.CountCapStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.GraphComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.UnrollJumpStrategy;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GiraphRemoteAcceptor implements RemoteAcceptor {

    private GiraphGraph giraphGraph;
    private Groovysh shell;
    private boolean useSugarPlugin = false;
    private String graphVariable = "g";

    public GiraphRemoteAcceptor(final Groovysh shell) {
        this.shell = shell;
    }

    @Override
    public Object connect(final List<String> args) {
        if (args.size() == 0) {
            this.giraphGraph = GiraphGraph.open(new BaseConfiguration());
            this.shell.getInterp().getContext().setProperty("g", this.giraphGraph);
        }
        if (args.size() == 1) {
            try {
                final FileConfiguration configuration = new PropertiesConfiguration();
                configuration.load(new File(args.get(0)));
                this.giraphGraph = GiraphGraph.open(configuration);
                this.shell.getInterp().getContext().setProperty("g", this.giraphGraph);
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else if (args.size() == 2) {
            try {
                final FileConfiguration configuration = new PropertiesConfiguration();
                configuration.load(new File(args.get(0)));
                this.giraphGraph = GiraphGraph.open(configuration);
                this.graphVariable = args.get(1);
                this.shell.getInterp().getContext().setProperty(args.get(1), this.giraphGraph);
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        return this.giraphGraph;
    }

    @Override
    public Object configure(final List<String> args) {
        for (int i = 0; i < args.size(); i = i + 2) {
            if (args.get(i).equals("useSugar"))
                this.useSugarPlugin = Boolean.valueOf(args.get(i + 1));
            else {
                this.giraphGraph.variables().getConfiguration().setProperty(args.get(i), args.get(i + 1));
            }
        }
        return this.giraphGraph;
    }

    @Override
    public Object submit(final List<String> args) {
        try {
            final StringBuilder builder = new StringBuilder();
            if (this.useSugarPlugin) {
                // builder.append(InvokerHelper.class.getCanonicalName() + ".getMetaRegistry().removeMetaClass(" + GiraphGraph.class.getCanonicalName() + ".class)\n");
                builder.append(SugarLoader.class.getCanonicalName() + ".load()\n");
            }
            builder.append(this.graphVariable + " = " + GiraphGraph.class.getCanonicalName() + ".open()\n");
            builder.append("traversal = ");
            builder.append(String.join(" ", args));
            builder.append("\n");
            builder.append(GraphComputerPreparation.class.getCanonicalName() + ".prepare(traversal)\n");
            builder.append("traversal\n");

            final TraversalVertexProgram vertexProgram = TraversalVertexProgram.build().traversal(new GSSupplier<>(builder.toString())).create();
            final ComputerResult result = this.giraphGraph.compute().program(vertexProgram).submit().get();

            this.shell.getInterp().getContext().setProperty("result", result);

            final GraphTraversal traversal1 = new DefaultGraphTraversal<>();
            traversal1.addStep(new ComputerResultStep<>(traversal1, result, vertexProgram, false));
            this.shell.getInterp().getContext().setProperty("_l", traversal1);

            final GraphTraversal traversal2 = new DefaultGraphTraversal<>();
            traversal2.addStep(new ComputerResultStep<>(traversal2, result, vertexProgram, false));
            traversal2.range(0, 19);
            return traversal2;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.giraphGraph.close();
    }

    public GiraphGraph getGraph() {
        return this.giraphGraph;
    }

    public String toString() {
        return "GiraphRemoteAcceptor[" + this.giraphGraph + "]";
    }

    // this sucks as this has to ALWAYS be in sync with the submit requirements of GraphTraversal.submit().
    public static class GraphComputerPreparation {
        public static void prepare(final Traversal traversal) {
            traversal.sideEffects().removeGraph();
            traversal.strategies().unregister(UnrollJumpStrategy.class);
            traversal.strategies().unregister(TraverserSourceStrategy.class);
            traversal.strategies().register(CountCapStrategy.instance());
            traversal.strategies().register(GraphComputerStrategy.instance());
        }
    }
}
