package com.tinkerpop.gremlin.hadoop.process.computer.util;

import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.hadoop.process.computer.HadoopCombine;
import com.tinkerpop.gremlin.hadoop.process.computer.HadoopMap;
import com.tinkerpop.gremlin.hadoop.process.computer.HadoopReduce;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableComparator;
import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapReduceHelper {

    private static final String SEQUENCE_WARNING = "The " + Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT
            + " is not " + SequenceFileOutputFormat.class.getCanonicalName()
            + " and thus, graph computer memory can not be converted to Java objects";

    public static void executeMapReduceJob(final MapReduce mapReduce, final Memory.Admin memory, final Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration newConfiguration = new Configuration(configuration);
        final BaseConfiguration apacheConfiguration = new BaseConfiguration();
        mapReduce.storeState(apacheConfiguration);
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, newConfiguration);
        if (!mapReduce.doStage(MapReduce.Stage.MAP)) {
            final Path memoryPath = new Path(configuration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + mapReduce.getMemoryKey());
            if (newConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, SequenceFileOutputFormat.class, OutputFormat.class).equals(SequenceFileOutputFormat.class))
                mapReduce.addResultToMemory(memory, new ObjectWritableIterator(configuration, memoryPath));
            else
                HadoopGraph.LOGGER.warn(SEQUENCE_WARNING);
        } else {
            final Optional<Comparator<?>> mapSort = mapReduce.getMapKeySort();
            final Optional<Comparator<?>> reduceSort = mapReduce.getReduceKeySort();

            newConfiguration.setClass(Constants.GREMLIN_HADOOP_MAP_REDUCE_CLASS, mapReduce.getClass(), MapReduce.class);
            final Job job = new Job(newConfiguration, mapReduce.toString());
            HadoopGraph.LOGGER.info(Constants.GREMLIN_HADOOP_JOB_PREFIX + mapReduce.toString());
            job.setJarByClass(HadoopGraph.class);
            if (mapSort.isPresent())
                job.setSortComparatorClass(ObjectWritableComparator.ObjectWritableMapComparator.class);
            job.setMapperClass(HadoopMap.class);
            if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                if (mapReduce.doStage(MapReduce.Stage.COMBINE))
                    job.setCombinerClass(HadoopCombine.class);
                job.setReducerClass(HadoopReduce.class);
            } else {
                if (mapSort.isPresent()) {
                    job.setReducerClass(Reducer.class);
                } else {
                    job.setNumReduceTasks(0);
                }
            }
            job.setMapOutputKeyClass(ObjectWritable.class);
            job.setMapOutputValueClass(ObjectWritable.class);
            job.setOutputKeyClass(ObjectWritable.class);
            job.setOutputValueClass(ObjectWritable.class);
            job.setInputFormatClass((Class) newConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class));
            job.setOutputFormatClass(newConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class, OutputFormat.class)); // TODO: Make this configurable
            // if there is no vertex program, then grab the graph from the input location
            final Path graphPath = configuration.get(VertexProgram.VERTEX_PROGRAM, null) != null ?
                    new Path(newConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + Constants.SYSTEM_G) :
                    new Path(newConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
            Path memoryPath = new Path(newConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + (reduceSort.isPresent() ? mapReduce.getMemoryKey() + "-temp" : mapReduce.getMemoryKey()));
            if (FileSystem.get(newConfiguration).exists(memoryPath)) {
                FileSystem.get(newConfiguration).delete(memoryPath, true);
            }
            FileInputFormat.setInputPaths(job, graphPath);
            FileOutputFormat.setOutputPath(job, memoryPath);
            job.waitForCompletion(true);


            // if there is a reduce sort, we need to run another identity MapReduce job
            if (reduceSort.isPresent()) {
                final Job reduceSortJob = new Job(newConfiguration, "ReduceKeySort");
                reduceSortJob.setSortComparatorClass(ObjectWritableComparator.ObjectWritableReduceComparator.class);
                reduceSortJob.setMapperClass(Mapper.class);
                reduceSortJob.setReducerClass(Reducer.class);
                reduceSortJob.setMapOutputKeyClass(ObjectWritable.class);
                reduceSortJob.setMapOutputValueClass(ObjectWritable.class);
                reduceSortJob.setOutputKeyClass(ObjectWritable.class);
                reduceSortJob.setOutputValueClass(ObjectWritable.class);
                reduceSortJob.setInputFormatClass(SequenceFileInputFormat.class); // TODO: require this hard coded? If so, ERROR messages needed.
                reduceSortJob.setOutputFormatClass(newConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class, OutputFormat.class));
                FileInputFormat.setInputPaths(reduceSortJob, memoryPath);
                final Path sortedMemoryPath = new Path(newConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + mapReduce.getMemoryKey());
                FileOutputFormat.setOutputPath(reduceSortJob, sortedMemoryPath);
                reduceSortJob.waitForCompletion(true);
                FileSystem.get(newConfiguration).delete(memoryPath, true); // delete the temporary memory path
                memoryPath = sortedMemoryPath;
            }

            // if its not a SequenceFile there is no certain way to convert to necessary Java objects.
            // to get results you have to look through HDFS directory structure. Oh the horror.
            if (newConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class, OutputFormat.class).equals(SequenceFileOutputFormat.class))
                mapReduce.addResultToMemory(memory, new ObjectWritableIterator(configuration, memoryPath));
            else
                HadoopGraph.LOGGER.warn(SEQUENCE_WARNING);
        }
    }
}
