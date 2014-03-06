package com.tinkerpop.gremlin.giraph.process.olap;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputerTest {

    @Test
    public void testPlay() {
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty("giraph.vertexClass", SimpleShortestPathsVertex.class.getName());
        configuration.setProperty("giraph.vertexInputFormatClass", JsonLongDoubleFloatDoubleVertexInputFormat.class.getName());
        configuration.setProperty("giraph.vertexOutputFormatClass", IdWithValueTextOutputFormat.class.getName());
        configuration.setProperty("giraph.maxWorkers", "1");
        configuration.setProperty("giraph.SplitMasterWorker", "false");
        configuration.setProperty("mapred.job.tracker", "localhost:9001");
        configuration.setProperty("giraph.vertex.input.dir", "tiny_graph.txt");
        configuration.setProperty("mapred.output.dir", "output");

        //GiraphGraphComputer g = new GiraphGraphComputer();
        //g.configuration(configuration).submit();
    }
}
