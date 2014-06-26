package com.tinkerpop.gremlin.giraph.process;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoInputFormat;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;
import org.junit.Test;

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
        baseConfiguration.setProperty("giraph.zkJar", GiraphGremlinInputFormat.class.getResource("zookeeper-3.3.3.jar").getPath());
        baseConfiguration.setProperty("gremlin.inputLocation", KryoInputFormat.class.getResource("tinkerpop-classic-vertices.gio").getPath());
        baseConfiguration.setProperty("gremlin.outputLocation", "giraph-gremlin/target/test-output");
    }

    @Test
    @Ignore
    public void shouldSerializeTraversalCountersAndItDoesNot() {
        final GiraphGraph g = GiraphGraph.open(baseConfiguration);
        //System.out.println(g.v(1).value("name").toString());
        // g.V().forEach(v -> System.out.println(v.getClass()));
        g.V().out().out().submit(g.compute()).in().forEach(System.out::println);

    }

    /*@Test
    public void shouldWork2() {
        final GiraphGraph g = GiraphGraph.open(baseConfiguration);
        Iterator<Vertex> vertices = new VertexIterator(new KryoVertexInputFormat(), ConfUtil.makeHadoopConfiguration(baseConfiguration));
        vertices.forEachRemaining(System.out::println);
    }*/

}
