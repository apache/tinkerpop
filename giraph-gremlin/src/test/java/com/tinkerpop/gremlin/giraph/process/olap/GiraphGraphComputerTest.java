package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgram;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputerTest {

    @Test
    public void testPlay() {
        GraphComputer g = new GiraphGraphComputer();
        /*final Configuration configuration = PageRankVertexProgram.create().getConfiguration();
        configuration.setProperty("mapred.output.dir", "output");
        configuration.setProperty("giraph.vertexInputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphInputFormat");
        configuration.setProperty("giraph.vertexOutputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphOutputFormat");
        configuration.setProperty("giraph.minWorkers", 1);
        configuration.setProperty("giraph.maxWorkers", 1);
        configuration.setProperty("gremlin.input.location", "../data/grateful-dead.xml");
        configuration.setProperty("giraph.SplitMasterWorker", false);
        //configuration.setProperty("giraph.localTestMode",true);
        g.program(configuration).submit();*/
    }
}
