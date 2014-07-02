package com.tinkerpop.gremlin.giraph.groovy.plugin;

import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.hdfs.TextFileLineIterator;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.groovy.engine.function.GremlinGroovySSupplier;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.javatuples.Pair;

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

    private GiraphGraph graph;
    private Groovysh shell;

    public GiraphRemoteAcceptor(final Groovysh shell) {
        this.shell = shell;
    }

    @Override
    public Object connect(final List<String> args) {
        if (args.size() == 0) {
            this.graph = GiraphGraph.open(new BaseConfiguration());
            this.shell.getInterp().getContext().setProperty("g", this.graph);
        }
        if (args.size() == 1) {
            try {
                final FileConfiguration configuration = new PropertiesConfiguration();
                configuration.load(new File(args.get(0)));
                this.graph = GiraphGraph.open(configuration);
                this.shell.getInterp().getContext().setProperty("g", this.graph);
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else if (args.size() == 2) {
            try {
                final FileConfiguration configuration = new PropertiesConfiguration();
                configuration.load(new File(args.get(0)));
                this.graph = GiraphGraph.open(configuration);
                this.shell.getInterp().getContext().setProperty(args.get(1), this.graph);
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        return this.graph;
    }

    @Override
    public Object configure(final List<String> args) {
        for (int i = 0; i < args.size(); i = i + 2) {
            this.graph.variables().<Configuration>get(GiraphGraph.CONFIGURATION).setProperty(args.get(i), args.get(i + 1));
        }
        return this.graph;
    }

    @Override
    public Object submit(final List<String> args) {
        try {
            VertexProgramHelper.serializeSupplier(new GremlinGroovySSupplier<>(PREFIX_SCRIPT + args.get(0)), this.graph.variables().<Configuration>get(GiraphGraph.CONFIGURATION), TraversalVertexProgram.TRAVERSAL_SUPPLIER);
            final Pair<Graph, GraphComputer.Globals> result = this.graph.compute().program(this.graph.variables().<Configuration>get(GiraphGraph.CONFIGURATION)).submit().get();
            final Optional<Iterator<String>> capResult = GiraphRemoteAcceptor.getCapIterator(this.graph.variables().<Configuration>get(GiraphGraph.CONFIGURATION));
            this.shell.getInterp().getContext().setProperty("g", result.getValue0());
            if (capResult.isPresent())
                return capResult.get();
            else
                return result.getValue0();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.graph.close();
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
        return this.graph;
    }

    public String toString() {
        return "GiraphRemoteAcceptor[" + this.graph + "]";
    }
}
