package com.tinkerpop.gremlin.giraph.process.olap;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunner extends Configured implements Tool {

    private final GiraphConfiguration giraphConfiguration;

    public GiraphGraphRunner(final GiraphConfiguration giraphConfiguration) {
        this.giraphConfiguration = giraphConfiguration;
    }

    public int run(final String[] args) {
        try {
            final GiraphJob job = new GiraphJob(this.giraphConfiguration, "GiraphGraph Play");
            job.getInternalJob().setJarByClass(GiraphGraphComputer.class);
            job.run(true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    public static void main(final String[] args) {
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty("giraph.vertexClass", SimpleShortestPathsVertex.class.getName());
        configuration.setProperty("giraph.vertexInputFormatClass", JsonLongDoubleFloatDoubleVertexInputFormat.class.getName());
        configuration.setProperty("giraph.vertexOutputFormatClass", IdWithValueTextOutputFormat.class.getName());
        configuration.setProperty("giraph.maxWorkers", "1");
        configuration.setProperty("giraph.SplitMasterWorker", "false");
        configuration.setProperty("mapred.job.tracker", "localhost:9001");
        configuration.setProperty("giraph.vertex.input.dir", "tiny_graph.txt");
        configuration.setProperty("mapred.output.dir", "output");

        GiraphGraphComputer g = new GiraphGraphComputer();
        g.configuration(configuration).submit();
    }
}
