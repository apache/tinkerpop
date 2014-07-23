package com.tinkerpop.gremlin.giraph.process;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoInputFormat;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerMapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerStep;
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
        //g.V().out().out().value("name").forEach(System.out::println);
        //g.variables().<Configuration>get(GiraphGraph.CONFIGURATION).getKeys().forEachRemaining(key -> System.out.println(key + "--" +  g.variables().<Configuration>get(GiraphGraph.CONFIGURATION).getString(key)));
        g.V().value("name").groupCount().submit(g.compute()).forEach(System.out::println);

        //g.V().value("name").groupCount().submit(g.compute()).forEach(System.out::println);
        //final GiraphGraph h = g.getOutputGraph();
        //h.variables().<Configuration>get(GiraphGraph.CONFIGURATION).getKeys().forEachRemaining(key -> System.out.println(key + "--" +  h.variables().<Configuration>get(GiraphGraph.CONFIGURATION).getString(key)));
        //h.V().out().forEach(System.out::println);


    }

    /*@Test
    public void shouldWork2() throws Exception {
        SideEffectCapComputerMapReduce.class.getConstructor().newInstance();
    }*/

}
