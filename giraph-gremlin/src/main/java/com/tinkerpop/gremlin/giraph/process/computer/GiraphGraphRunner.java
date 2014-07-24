package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.giraph.process.computer.util.SideEffectsMapReduce;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunner extends Configured implements Tool {

    private final GiraphConfiguration giraphConfiguration;
    public static final Logger LOGGER = Logger.getLogger(GiraphGraphRunner.class);
    private SideEffects sideEffects;

    public GiraphGraphRunner(final org.apache.hadoop.conf.Configuration hadoopConfiguration, final GiraphGraphShellComputerSideEffects sideEffects) {
        this.giraphConfiguration = new GiraphConfiguration();
        hadoopConfiguration.forEach(entry -> this.giraphConfiguration.set(entry.getKey(), entry.getValue()));
        this.giraphConfiguration.setMasterComputeClass(GiraphGraphComputerSideEffects.class);
        this.giraphConfiguration.setVertexClass(GiraphInternalVertex.class);
        this.giraphConfiguration.setOutEdgesClass(EmptyOutEdges.class);
        this.giraphConfiguration.setClass("giraph.vertexIdClass", LongWritable.class, LongWritable.class);
        this.giraphConfiguration.setClass("giraph.vertexValueClass", Text.class, Text.class);
        ///
        this.sideEffects = sideEffects;
    }

    public int run(final String[] args) {
        try {
            final VertexProgram vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.giraphConfiguration));
            final GiraphJob job = new GiraphJob(this.giraphConfiguration, Constants.GIRAPH_GREMLIN_JOB_PREFIX + vertexProgram);
            //job.getInternalJob().setJarByClass(GiraphGraphComputer.class);
            FileInputFormat.setInputPaths(job.getInternalJob(), new Path(this.giraphConfiguration.get(Constants.GREMLIN_INPUT_LOCATION)));
            FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(this.giraphConfiguration.get(Constants.GREMLIN_OUTPUT_LOCATION) + "/" + Constants.TILDA_G));
            LOGGER.info(Constants.GIRAPH_GREMLIN_JOB_PREFIX + vertexProgram);
            job.run(true);

            final List<MapReduce> mapReduces = new ArrayList<MapReduce>(vertexProgram.getMapReducers());
            // calculate main traversal vertex program sideEffect variables
            if (this.giraphConfiguration.getBoolean(Constants.GREMLIN_DERIVE_MAIN_SIDE_EFFECTS, false)) {
                final Set<String> sideEffectKeys = new HashSet<String>(vertexProgram.getSideEffectKeys());
                sideEffectKeys.add(Constants.RUNTIME);
                sideEffectKeys.add(Constants.ITERATION);
                this.giraphConfiguration.setStrings(Constants.GREMLIN_SIDE_EFFECT_KEYS, (String[]) sideEffectKeys.toArray(new String[sideEffectKeys.size()]));
                mapReduces.add(new SideEffectsMapReduce(sideEffectKeys));
            }
            // do extra map reduce jobs if necessary
            for (final MapReduce mapReduce : mapReduces) {
                MapReduceHelper.executeMapReduceJob(mapReduce, this.sideEffects, this.giraphConfiguration);
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
