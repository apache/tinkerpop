package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.process.olap.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.File;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunner extends Configured implements Tool {

    private static final String GIRAPH_VERTEX_CLASS = "giraph.vertexClass";

    private final GiraphConfiguration giraphConfiguration;

    public GiraphGraphRunner(final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        this.giraphConfiguration = new GiraphConfiguration(hadoopConfiguration);
        this.giraphConfiguration.setMasterComputeClass(GiraphComputerMemory.class);
        this.giraphConfiguration.setClass(GIRAPH_VERTEX_CLASS, GiraphVertex.class, Vertex.class);
    }

    public int run(final String[] args) {
        try {
            final GiraphJob job = new GiraphJob(this.giraphConfiguration,
                    "GiraphGremlin: " + VertexProgram.createVertexProgram(ConfUtil.apacheConfiguration(this.giraphConfiguration)));
            job.getInternalJob().setJarByClass(GiraphJob.class);
            job.run(true);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        try {
            final FileConfiguration configuration = new PropertiesConfiguration();
            configuration.load(new File(args[0]));
            GraphComputer computer = new GiraphGraphComputer();
            computer.program(configuration).submit();
        } catch (Exception e) {
            System.out.println(e);
            throw e;
        }
    }
}
