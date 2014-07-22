package com.tinkerpop.gremlin.giraph.groovy.plugin;

import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.hdfs.TextFileLineIterator;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.groovy.engine.function.GremlinGroovySSupplier;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgramIterator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GiraphRemoteAcceptor implements RemoteAcceptor {

    private static final String PREFIX_SCRIPT =
            "import " + GiraphGraph.class.getPackage().getName() + ".*\n" +
                    "g = GiraphGraph.open()\n";

    //TODO: might not always be 'g' cause of the variable bindings

    private GiraphGraph giraphGraph;
    private Groovysh shell;

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
            this.giraphGraph.variables().<Configuration>get(GiraphGraph.CONFIGURATION).setProperty(args.get(i), args.get(i + 1));
        }
        return this.giraphGraph;
    }

    @Override
    public Object submit(final List<String> args) {
        try {
            //VertexProgramHelper.serializeSupplier(new GremlinGroovySSupplier<>(PREFIX_SCRIPT + args.get(0)), this.graph.variables().getConfiguration(), TraversalVertexProgram.TRAVERSAL_SUPPLIER);
            final TraversalVertexProgramIterator iterator = new TraversalVertexProgramIterator(this.giraphGraph, new GremlinGroovySSupplier<>(PREFIX_SCRIPT + args.get(0)));
            this.shell.getInterp().getContext().setProperty("g", iterator.getResultantGraph());
            return iterator;
            /*final Pair<Graph, GraphComputer.Globals> result = this.graph.compute().program(this.graph.variables().getConfiguration()).submit().get();
            final Optional<Iterator<String>> capResult = GiraphRemoteAcceptor.getCapIterator(this.graph.variables().getConfiguration());
            this.shell.getInterp().getContext().setProperty("g", result.getValue0());
            if (capResult.isPresent())
                return capResult.get();
            else
                return new TraverserRes result.getValue0();*/
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.giraphGraph.close();
    }

    private static Optional<Iterator<String>> getCapIterator(final Configuration configuration) {
        try {
            if (configuration.containsKey(GiraphGraph.GREMLIN_OUTPUT_LOCATION)) {
                final String output = configuration.getString(GiraphGraph.GREMLIN_OUTPUT_LOCATION);
                final FileSystem fs = FileSystem.get(ConfUtil.makeHadoopConfiguration(configuration));
                final Path capOutput = new Path(output + "/" + SideEffectCapable.CAP_KEY);
                //final Path traversalResultOutput = new Path(output + "/" + "traversalResult");
                if (fs.exists(capOutput)) {
                    final LinkedList<Path> paths = new LinkedList<>();
                    paths.addAll(HDFSTools.getAllFilePaths(fs, capOutput, HiddenFileFilter.instance()));
                    return Optional.of(new TextFileLineIterator(fs, paths, 25));
                } /*else if (fs.exists(traversalResultOutput)) {
                    final LinkedList<Path> paths = new LinkedList<>();
                    paths.addAll(HDFSTools.getAllFilePaths(fs, traversalResultOutput, NoUnderscoreFilter.instance()));
                    return Optional.of(new TextFileLineIterator(fs, paths, 25));
                }*/
            }
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return Optional.empty();
    }

    public GiraphGraph getGraph() {
        return this.giraphGraph;
    }

    public String toString() {
        return "GiraphRemoteAcceptor[" + this.giraphGraph + "]";
    }
}
