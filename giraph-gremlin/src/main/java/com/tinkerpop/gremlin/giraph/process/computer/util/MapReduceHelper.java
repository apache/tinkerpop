package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.hdfs.KeyHelper;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphMap;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphReduce;
import com.tinkerpop.gremlin.giraph.process.computer.KryoWritable;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerMapReduce;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapReduceHelper {

    public static final String MAP_REDUCE_CLASS = "gremlin.mapReduceClass";

    public static void executeMapReduceJob(final MapReduce mapReduce, final GraphComputer.Globals globals, final Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration newConfiguration = new Configuration(configuration);
        final org.apache.commons.configuration.Configuration apacheConfiguration = new BaseConfiguration();
        mapReduce.stageConfiguration(apacheConfiguration);
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, newConfiguration);
        if (mapReduce instanceof SideEffectCapComputerMapReduce) {
            final Path sideEffectPath = new Path(configuration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(mapReduce.getResultVariable()));
            storeSideEffectResults(mapReduce, globals, sideEffectPath, configuration);
        } else {
            newConfiguration.setClass(MAP_REDUCE_CLASS, mapReduce.getClass(), MapReduce.class);
            final Job job = new Job(newConfiguration, mapReduce.toString());
            job.setJarByClass(GiraphGraph.class);
            job.setMapperClass(GiraphMap.class);
            //extraJob.setCombinerClass(Combiner.class);
            if (mapReduce.doReduce())
                job.setReducerClass(GiraphReduce.class);
            else
                job.setNumReduceTasks(0);
            job.setMapOutputKeyClass(KryoWritable.class);
            job.setMapOutputValueClass(KryoWritable.class);
            job.setOutputKeyClass(KryoWritable.class);
            job.setOutputValueClass(KryoWritable.class);
            job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) newConfiguration.getClass(GiraphGraph.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
            job.setOutputFormatClass(SequenceFileOutputFormat.class); // TODO: Make this configurable
            final Path graphPath = new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G);
            final Path sideEffectPath = new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(mapReduce.getResultVariable()));
            FileInputFormat.setInputPaths(job, graphPath);
            FileOutputFormat.setOutputPath(job, sideEffectPath);
            job.waitForCompletion(true);
            storeSideEffectResults(mapReduce, globals, sideEffectPath, configuration);
        }
    }

    public static void storeSideEffectResults(final MapReduce mapReduce, final GraphComputer.Globals globals, final Path sideEffectPath, final Configuration configuration) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        final List list = new ArrayList();
        final KryoWritable key = new KryoWritable();
        final KryoWritable value = new KryoWritable();
        for (final FileStatus status : fs.listStatus(sideEffectPath, new HiddenFileFilter())) {
            final SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), configuration);
            while (reader.next(key, value)) {
                list.add(new Pair<>(key.get(), value.get()));
            }
        }
        globals.set(mapReduce.getResultVariable(), mapReduce.getResult(list.iterator()));   // TODO: Don't aggregate to list, but make lazy
    }
}
