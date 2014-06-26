package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.ExtraJobsCalculator;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunner extends Configured implements Tool {

    private final GiraphConfiguration giraphConfiguration;
    private static final Logger LOGGER = Logger.getLogger(GiraphGraphRunner.class);

    public GiraphGraphRunner(final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        this.giraphConfiguration = new GiraphConfiguration();
        hadoopConfiguration.forEach(entry -> this.giraphConfiguration.set(entry.getKey(), entry.getValue()));
        this.giraphConfiguration.setMasterComputeClass(GiraphGraphComputerGlobals.class);
        this.giraphConfiguration.setVertexClass(GiraphInternalVertex.class);
        this.giraphConfiguration.setOutEdgesClass(EmptyOutEdges.class);
        this.giraphConfiguration.setClass("giraph.vertexIdClass", LongWritable.class, LongWritable.class);
        this.giraphConfiguration.setClass("giraph.vertexValueClass", Text.class, Text.class);
    }

    public int run(final String[] args) {
        try {
            final VertexProgram vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.giraphConfiguration));
            final GiraphJob job = new GiraphJob(this.giraphConfiguration, GiraphGraphComputer.GIRAPH_GREMLIN_JOB_PREFIX + vertexProgram);
            //job.getInternalJob().setJarByClass(GiraphGraphComputer.class);
            FileInputFormat.setInputPaths(job.getInternalJob(), new Path(this.giraphConfiguration.get(GiraphGraphComputer.GREMLIN_INPUT_LOCATION)));
            FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(this.giraphConfiguration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G));
            LOGGER.info(GiraphGraphComputer.GIRAPH_GREMLIN_JOB_PREFIX + vertexProgram);
            job.run(true);
            // calculate global variables
            if (this.giraphConfiguration.getBoolean(GiraphGraphComputer.GREMLIN_DERIVE_GLOBALS, false)) {
                final Set<String> globalKeys = new HashSet<String>(vertexProgram.getGlobalKeys());
                globalKeys.add(GlobalsMapReduce.RUNTIME);
                globalKeys.add(GlobalsMapReduce.ITERATION);
                this.giraphConfiguration.setStrings(GlobalsMapReduce.GREMLIN_GLOBAL_KEYS, (String[]) globalKeys.toArray(new String[globalKeys.size()]));
                final Job globalDerivationJob = new GlobalsMapReduce().createJob(this.giraphConfiguration);
                LOGGER.info(globalDerivationJob.getJobName());
                globalDerivationJob.waitForCompletion(true);
            }
            // do extra map reduce jobs if necessary
            if (null != this.giraphConfiguration.get(GiraphGraphComputer.GREMLIN_EXTRA_JOBS_CALCULATOR, null)) {
                final Class<ExtraJobsCalculator> calculator = (Class) this.giraphConfiguration.getClass(GiraphGraphComputer.GREMLIN_EXTRA_JOBS_CALCULATOR, ExtraJobsCalculator.class);

                final List<Job> extendedJobs = calculator.getConstructor().newInstance().deriveExtraJobs(this.giraphConfiguration);
                for (final Job extendedJob : extendedJobs) {
                    LOGGER.info(extendedJob.getJobName());
                    extendedJob.waitForCompletion(true);
                }
            }
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
            GraphComputer computer = new GiraphGraphComputer(GiraphGraph.open(), configuration);
            computer.program(configuration).submit().get();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
