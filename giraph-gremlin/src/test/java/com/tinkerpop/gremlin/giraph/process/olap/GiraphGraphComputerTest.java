package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphInputFormat;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgram;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputerTest {

    @Test
    public void testPlay() {
        /*Configuration configuration = new BaseConfiguration();
        configuration.setProperty("giraph.vertexClass", GiraphVertex.class.getName());
        configuration.setProperty("giraph.vertexInputFormatClass", TinkerGraphInputFormat.class.getName());
        configuration.setProperty("giraph.vertexOutputFormatClass", IdWithValueTextOutputFormat.class.getName());
        configuration.setProperty("giraph.maxWorkers", "1");
        //configuration.setProperty("giraph.zkList", "127.0.0.1:2181");
        configuration.setProperty("giraph.SplitMasterWorker", "false");
        //configuration.setProperty("mapred.job.tracker", "localhost:9001");
        //configuration.setProperty("giraph.vertex.input.dir", "tiny_graph.txt");
        configuration.setProperty("mapred.output.dir", "output");

        GraphComputer g = new GiraphGraphComputer();
        g.program(new PageRankVertexProgram.Builder().build()).configuration(configuration).submit(); */
    }
}
