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
import org.javatuples.Pair;

import java.io.File;
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

    private Configuration configuration;

    @Override
    public Object connect(final List<String> args) {
        if (args.size() == 0) {
            this.configuration = new BaseConfiguration();
            return GiraphGraph.open(this.configuration);
        } else {
            try {
                this.configuration = new PropertiesConfiguration();
                ((FileConfiguration) this.configuration).load(new File(args.get(0)));
                return GiraphGraph.open(this.configuration);
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    @Override
    public Object configure(final List<String> args) {
        for (int i = 0; i < args.size(); i = i + 2) {
            this.configuration.setProperty(args.get(i), args.get(i + 1));
        }
        return GiraphGraph.open(this.configuration);
    }

    @Override
    public Object submit(final List<String> args) {
        try {
            VertexProgramHelper.serializeSupplier(new GremlinGroovySSupplier<>(PREFIX_SCRIPT + args.get(0)), this.configuration, TraversalVertexProgram.TRAVERSAL_SUPPLIER);
            final Pair<Graph, GraphComputer.Globals> result = GiraphGraph.open(this.configuration).compute().program(this.configuration).submit().get();
            final Optional<Iterator<String>> capResult = GiraphRemoteAcceptor.getCapIterator(this.configuration);
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
        this.configuration.clear();
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
