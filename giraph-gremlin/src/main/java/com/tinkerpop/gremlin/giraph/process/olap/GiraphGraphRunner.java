package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.TinkerGraphInputFormat;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.structure.util.EmptyGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunner extends Configured implements Tool {

    private final GiraphConfiguration giraphConfiguration;

    public GiraphGraphRunner(final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        this.giraphConfiguration = new GiraphConfiguration(hadoopConfiguration);
        this.giraphConfiguration.setMasterComputeClass(GiraphComputerMemory.class);
        this.giraphConfiguration.setWorkerConfiguration(1, 1, 100.0f);
        // this.giraphConfiguration.setClass("gremlin.messageClass", Double.class, Double.class);
    }

    public int run(final String[] args) {
        try {
            final GiraphJob job = new GiraphJob(this.giraphConfiguration, "GiraphGraph Play");
            job.getInternalJob().setJarByClass(GiraphJob.class);
            job.run(true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    public static void main(final String[] args) {
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty("giraph.vertexClass", GiraphVertex.class.getName());
        configuration.setProperty("giraph.vertexInputFormatClass", TinkerGraphInputFormat.class.getName());
        configuration.setProperty("giraph.vertexOutputFormatClass", IdWithValueTextOutputFormat.class.getName()); // TODO:
        // configuration.setProperty("giraph.maxWorkers", "1");
        //configuration.setProperty("giraph.zkList", "127.0.0.1:2181");
        configuration.setProperty("giraph.SplitMasterWorker", "false");
        configuration.setProperty("mapred.job.tracker", "localhost:9001");
        // configuration.setProperty("giraph.vertex.input.dir", "tiny_graph.txt");
        configuration.setProperty("mapred.output.dir", "output");
        //configuration.setProperty("gremlin.messageClass", Double.class);

        GraphComputer g = new GiraphGraphComputer();
        //g.program(new PageRankVertexProgram.Builder().build()).configuration(configuration).submit();
        g.program(new TraversalVertexProgram.Builder().traversal(() -> EmptyGraph.instance().V()).build()).configuration(configuration).submit();
    }
}
