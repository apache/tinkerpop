package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.File;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunner extends Configured implements Tool {

    private static final String GIRAPH_VERTEX_CLASS = "giraph.vertexClass";
    private static final String GIRAPH_GREMLIN_TRAVERSAL_SUPPLIER = "giraph.gremlin.traversalSupplier";

    private final GiraphConfiguration giraphConfiguration;

    public GiraphGraphRunner(final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        this.giraphConfiguration = new GiraphConfiguration(hadoopConfiguration);
        this.giraphConfiguration.setMasterComputeClass(GiraphComputerMemory.class);
    }

    public int run(final String[] args) {
        try {
            final GiraphJob job = new GiraphJob(this.giraphConfiguration,
                    "GiraphGremlin: " + createTraversalSupplier(this.giraphConfiguration.get(GIRAPH_GREMLIN_TRAVERSAL_SUPPLIER)).get());
            job.getInternalJob().setJarByClass(GiraphJob.class);
            job.run(true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        try {
            FileConfiguration configuration = new PropertiesConfiguration();
            configuration.load(new File(args[0]));
            configuration.setProperty(GIRAPH_VERTEX_CLASS, GiraphVertex.class.getName());
            if (configuration.containsKey(GIRAPH_GREMLIN_TRAVERSAL_SUPPLIER)) {
                final SSupplier<Traversal> traversalSupplier = createTraversalSupplier(configuration.getProperty(GIRAPH_GREMLIN_TRAVERSAL_SUPPLIER).toString());
                GraphComputer g = new GiraphGraphComputer();
                g.program(new TraversalVertexProgram.Builder().traversal(traversalSupplier)).configuration(configuration).submit();
            } else {
                throw new RuntimeException("Only Gremlin traversals are currently supported");
            }
        } catch (Exception e) {
            System.out.println(e);
            throw e;
        }
        //g.program(new PageRankVertexProgram.Builder(configuration).build()).configuration(configuration).submit();
    }

    private static SSupplier<Traversal> createTraversalSupplier(final String giraphGremlinTraversalSupplierClass) throws Exception {
        return (SSupplier<Traversal>) Class.forName(giraphGremlinTraversalSupplierClass)
                .getConstructor()
                .newInstance();
    }
}
