package com.tinkerpop.gremlin.giraph.groovy.plugin;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.groovy.engine.function.GremlinGroovySSupplier;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GiraphRemoteAcceptor implements RemoteAcceptor {

    private static final String PREFIX_SCRIPT =
            "import " + GiraphGraph.class.getPackage().getName() + ".*\n" +
                    "g = GiraphGraph.open()\n";

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
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty("giraph.vertexInputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.graphson.GraphSONVertexInputFormat");
        configuration.setProperty("giraph.vertexOutputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.graphson.GraphSONVertexOutputFormat");
        configuration.setProperty("giraph.minWorkers", "2");
        configuration.setProperty("giraph.maxWorkers", "2");
        configuration.setProperty("gremlin.inputLocation", "tinkerpop-classic-adjlist.ldjson");
        configuration.setProperty("gremlin.outputLocation", "output");
        configuration.setProperty("gremlin.vertexProgram", "com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram");
        configuration.setProperty("gremlin.extraJobsCalculator", "com.tinkerpop.gremlin.giraph.process.TraversalExtraJobsCalculator");
        configuration.setProperty("gremlin.deriveGlobals", "false");
        try {
            VertexProgramHelper.serializeSupplier(new GremlinGroovySSupplier<>(PREFIX_SCRIPT + args.get(0)), configuration, "gremlin.traversalSupplier");
            return GiraphGraph.open().compute().program(configuration).submit().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
