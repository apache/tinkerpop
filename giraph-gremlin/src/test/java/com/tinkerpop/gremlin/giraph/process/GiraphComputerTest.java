package com.tinkerpop.gremlin.giraph.process;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerTest {

    final static Configuration baseConfiguration = new BaseConfiguration();

    static {
        baseConfiguration.setProperty("giraph.vertexInputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoVertexInputFormat");
        baseConfiguration.setProperty("giraph.vertexOutputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoVertexOutputFormat");
        baseConfiguration.setProperty("giraph.minWorkers", "1");
        baseConfiguration.setProperty("giraph.maxWorkers", "1");
        baseConfiguration.setProperty("giraph.SplitMasterWorker", "false");
        //baseConfiguration.setProperty("giraph.localTestMode", "true");
        baseConfiguration.setProperty("gremlin.extraJobsCalculator", "com.tinkerpop.gremlin.giraph.process.TraversalExtraJobsCalculator");
        baseConfiguration.setProperty("giraph.zkJar", "/Users/marko/software/tinkerpop/tinkerpop3/giraph-gremlin/target/giraph-gremlin-3.0.0-SNAPSHOT-standalone/lib/zookeeper-3.3.3.jar");
        baseConfiguration.setProperty("gremlin.inputLocation", "data/tinkerpop-classic-vertices.gio");
        baseConfiguration.setProperty("gremlin.outputLocation", "giraph-gremlin/target/test-output");
    }

    @Test
    @Ignore
    public void shouldSerializeTraversalCountersAndItDoesNot() {
        // read the standard adjacency list tinkerpop-classic from disk
        final GiraphGraph g = GiraphGraph.open(baseConfiguration);

        // run a Giraph job that does a graph traversal and store the graph in gremlin.outputLocation
        g.V().out().value("name").submit(g.compute());

        // read the Giraph graph from gremlin.outputLocation with the traversal counters on it and it no likey!
        g.V().map(v -> v.get().hiddens()).forEach(System.out::println);
    }

    @Test
    public void shouldSerializeTraversalCountersAndItDoes() throws Exception {
        // read the graph normally from data/ directory
        Graph g = TinkerGraph.open();
        KryoReader.create().build().readGraph(new FileInputStream("data/tinkerpop-classic.gio"), g);

        // do a traversal and now traversal counters exist on the vertices; write the graph to disk
        g = g.compute().program(TraversalVertexProgram.create().traversal(() -> TinkerGraph.open().V().out().value("name")).getConfiguration()).submit().get().getValue0();
        KryoWriter.create().build().writeGraph(new FileOutputStream("giraph-gremlin/target/temp.gio"), g);

        // read the graph from disk and see the traversal counters came with it. yes!
        g = TinkerGraph.open();
        KryoReader.create().build().readGraph(new FileInputStream("giraph-gremlin/target/temp.gio"), g);
        g.V().map(v -> v.get().hiddens()).forEach(System.out::println);
    }


    /*@Test
    public void shouldWork2() {
        final GiraphGraph g = GiraphGraph.open(baseConfiguration);
        Iterator<Vertex> vertices = new VertexIterator(new KryoVertexInputFormat(), ConfUtil.makeHadoopConfiguration(baseConfiguration));
        vertices.forEachRemaining(System.out::println);
    }*/

}
