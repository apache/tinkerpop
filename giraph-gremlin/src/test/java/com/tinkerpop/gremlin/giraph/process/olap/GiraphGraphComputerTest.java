package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.IntegrationTest;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgram;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.processing.ProcessingEnvironment;
import java.io.File;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Category(IntegrationTest.class)
public class GiraphGraphComputerTest {

    @Test
    public void testPlay() throws IOException {
        File f = new File("target");
        if (f.exists() && f.isDirectory()) {
            for (File h : f.listFiles()) {
                if (h.isDirectory() && h.getName().endsWith("standalone")) {
                    System.setProperty("GIRAPH_GREMLIN_HOME", h.getCanonicalPath());
                    break;
                }
            }
        }

        GraphComputer g = new GiraphGraphComputer();
        final Configuration configuration = PageRankVertexProgram.create().getConfiguration();
        configuration.setProperty("mapred.output.dir", "output");
        configuration.setProperty("giraph.vertexInputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphInputFormat");
        configuration.setProperty("giraph.vertexOutputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphOutputFormat");
        configuration.setProperty("giraph.minWorkers", 1);
        configuration.setProperty("giraph.maxWorkers", 1);
        configuration.setProperty("gremlin.input.location", "../data/grateful-dead.xml");
        configuration.setProperty("giraph.SplitMasterWorker", false);
        //configuration.setProperty("giraph.localTestMode",true);
        g.program(configuration).submit();
    }
}
