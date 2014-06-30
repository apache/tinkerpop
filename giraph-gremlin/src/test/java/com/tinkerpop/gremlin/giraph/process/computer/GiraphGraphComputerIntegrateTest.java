package com.tinkerpop.gremlin.giraph.process.computer;


import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputerIntegrateTest {

    @Test
    public void testPlay() throws IOException {
        Configuration x = new Configuration();
        x.iterator().forEachRemaining(System.out::println);

       /* File f = new File("target");
        if (f.exists() && f.isDirectory()) {
            for (File h : f.listFiles()) {
                if (h.isDirectory() && h.getName().endsWith("standalone")) {
                    System.setProperty("GIRAPH_GREMLIN_HOME", h.getCanonicalPath());
                    break;
                }
            }
        }

        GraphComputer g = new GiraphGraphComputer(GiraphGraph.open(), new BaseConfiguration());
        final Configuration configuration = PageRankVertexProgram.create().getConfiguration();
        configuration.setProperty("mapred.output.dir", "output");
        configuration.setProperty("giraph.vertexInputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphInputFormat");
        configuration.setProperty("giraph.vertexOutputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphOutputFormat");
        configuration.setProperty("giraph.minWorkers", 1);
        configuration.setProperty("giraph.maxWorkers", 1);
        configuration.setProperty("gremlin.input.location", "../data/grateful-dead.xml");
        configuration.setProperty("giraph.SplitMasterWorker", false);
        //configuration.setProperty("giraph.localTestMode",true);
        g.program(configuration).submit();  */


    }
}
