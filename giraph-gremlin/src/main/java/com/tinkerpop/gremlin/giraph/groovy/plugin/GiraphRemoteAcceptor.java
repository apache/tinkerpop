package com.tinkerpop.gremlin.giraph.groovy.plugin;

import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.NoUnderscoreFilter;
import com.tinkerpop.gremlin.giraph.hdfs.TextFileLineIterator;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.groovy.engine.function.GremlinGroovySSupplier;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GiraphRemoteAcceptor implements RemoteAcceptor {

    private static final String PREFIX_SCRIPT =
            "import " + GiraphGraph.class.getPackage().getName() + ".*\n" +
                    "g = GiraphGraph.open()\n";

    private static Configuration configuration = new BaseConfiguration();

    static {
        configuration.setProperty("giraph.vertexInputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.graphson.GraphSONVertexInputFormat");
        configuration.setProperty("giraph.vertexOutputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.graphson.GraphSONVertexOutputFormat");
        configuration.setProperty("giraph.minWorkers", "2");
        configuration.setProperty("giraph.maxWorkers", "2");
        configuration.setProperty("gremlin.inputLocation", "tinkerpop-classic-adjlist.ldjson");
        configuration.setProperty("gremlin.outputLocation", "output");
        configuration.setProperty("gremlin.vertexProgram", "com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram");
        configuration.setProperty("gremlin.extraJobsCalculator", "com.tinkerpop.gremlin.giraph.process.TraversalExtraJobsCalculator");
        configuration.setProperty("gremlin.deriveGlobals", "false");
    }

    @Override
    public Object connect(final List<String> args) {
        return null;
    }

    @Override
    public Object configure(final List<String> args) {
        return null;
    }

    @Override
    public Object submit(final List<String> args) {

        try {
            VertexProgramHelper.serializeSupplier(new GremlinGroovySSupplier<>(PREFIX_SCRIPT + args.get(0)), configuration, "gremlin.traversalSupplier");
            final Pair<Graph, GraphComputer.Globals> result = GiraphGraph.open().compute().program(configuration).submit().get();
            final Optional<Iterator<String>> capResult = GiraphRemoteAcceptor.getCapIterator(configuration);
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

    }

    private static Optional<Iterator<String>> getCapIterator(final Configuration configuration) {
        try {
            if (configuration.containsKey(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION)) {
                final String output = configuration.getString(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION);
                final FileSystem fs = FileSystem.get(ConfUtil.makeHadoopConfiguration(configuration));
                final Path capOutput = new Path(output + "/" + SideEffectCapable.CAP_VARIABLE);
                //final Path traversalResultOutput = new Path(output + "/" + "traversalResult");
                if (fs.exists(capOutput)) {
                    final LinkedList<Path> paths = new LinkedList<>();
                    paths.addAll(HDFSTools.getAllFilePaths(fs, capOutput, NoUnderscoreFilter.instance()));
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
}
