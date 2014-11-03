package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.hdfs.KryoWritableIterator;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphMap;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphReduce;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Comparator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapReduceHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceHelper.class);

    private static final String SEQUENCE_WARNING = "The " + Constants.GREMLIN_GIRAPH_MEMORY_OUTPUT_FORMAT_CLASS
            + " is not " + SequenceFileOutputFormat.class.getCanonicalName()
            + " and thus, graph computer memory can not be converted to Java objects";

    public static void executeMapReduceJob(final MapReduce mapReduce, final Memory memory, final Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration newConfiguration = new Configuration(configuration);
        final BaseConfiguration apacheConfiguration = new BaseConfiguration();
        mapReduce.storeState(apacheConfiguration);
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, newConfiguration);
        if (!mapReduce.doStage(MapReduce.Stage.MAP)) {
            final Path memoryPath = new Path(configuration.get(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION) + "/" + mapReduce.getMemoryKey());
            if (newConfiguration.getClass(Constants.GREMLIN_GIRAPH_MEMORY_OUTPUT_FORMAT_CLASS, SequenceFileOutputFormat.class, OutputFormat.class).equals(SequenceFileOutputFormat.class))
                mapReduce.addResultToMemory(memory, new KryoWritableIterator(configuration, memoryPath));
            else
                GiraphGraphComputer.LOGGER.warn(SEQUENCE_WARNING);
        } else {
            final Optional<Comparator<?>> mapSort = mapReduce.getMapKeySort();
            final Optional<Comparator<?>> reduceSort = mapReduce.getReduceKeySort();

            newConfiguration.setClass(Constants.GRELMIN_GIRAPH_MAP_REDUCE_CLASS, mapReduce.getClass(), MapReduce.class);
            final Job job = new Job(newConfiguration, mapReduce.toString());
            GiraphGraphComputer.LOGGER.info(Constants.GIRAPH_GREMLIN_JOB_PREFIX + mapReduce.toString());
            job.setJarByClass(GiraphGraph.class);
            if (mapSort.isPresent()) job.setSortComparatorClass(KryoWritableComparator.KryoWritableMapComparator.class);
            job.setMapperClass(GiraphMap.class);
            if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                if (mapReduce.doStage(MapReduce.Stage.COMBINE)) job.setCombinerClass(GiraphReduce.class);
                job.setReducerClass(GiraphReduce.class);
            } else {
                if (mapSort.isPresent()) {
                    job.setReducerClass(Reducer.class);
                } else {
                    job.setNumReduceTasks(0);
                }
            }
            job.setMapOutputKeyClass(KryoWritable.class);
            job.setMapOutputValueClass(KryoWritable.class);
            job.setOutputKeyClass(KryoWritable.class);
            job.setOutputValueClass(KryoWritable.class);
            job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) newConfiguration.getClass(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
            job.setOutputFormatClass(newConfiguration.getClass(Constants.GREMLIN_GIRAPH_MEMORY_OUTPUT_FORMAT_CLASS, SequenceFileOutputFormat.class, OutputFormat.class)); // TODO: Make this configurable
            // if there is no vertex program, then grab the graph from the input location
            final Path graphPath = configuration.get(VertexProgram.VERTEX_PROGRAM, null) != null ?
                    new Path(newConfiguration.get(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION) + "/" + Constants.SYSTEM_G) :
                    new Path(newConfiguration.get(Constants.GREMLIN_GIRAPH_INPUT_LOCATION));
            Path memoryPath = new Path(newConfiguration.get(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION) + "/" + (reduceSort.isPresent() ? mapReduce.getMemoryKey() + "-temp" : mapReduce.getMemoryKey()));
            if (FileSystem.get(newConfiguration).exists(memoryPath)) {
                FileSystem.get(newConfiguration).delete(memoryPath, true);
            }
            FileInputFormat.setInputPaths(job, graphPath);
            FileOutputFormat.setOutputPath(job, memoryPath);
            job.waitForCompletion(true);


            // if there is a reduce sort, we need to run another identity MapReduce job
            if (reduceSort.isPresent()) {
                final Job reduceSortJob = new Job(newConfiguration, "ReduceKeySort");
                reduceSortJob.setSortComparatorClass(KryoWritableComparator.KryoWritableReduceComparator.class);
                reduceSortJob.setMapperClass(Mapper.class);
                reduceSortJob.setReducerClass(Reducer.class);
                reduceSortJob.setMapOutputKeyClass(KryoWritable.class);
                reduceSortJob.setMapOutputValueClass(KryoWritable.class);
                reduceSortJob.setOutputKeyClass(KryoWritable.class);
                reduceSortJob.setOutputValueClass(KryoWritable.class);
                reduceSortJob.setInputFormatClass(SequenceFileInputFormat.class); // TODO: require this hard coded? If so, ERROR messages needed.
                reduceSortJob.setOutputFormatClass(newConfiguration.getClass(Constants.GREMLIN_GIRAPH_MEMORY_OUTPUT_FORMAT_CLASS, SequenceFileOutputFormat.class, OutputFormat.class));
                FileInputFormat.setInputPaths(reduceSortJob, memoryPath);
                final Path sortedMemoryPath = new Path(newConfiguration.get(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION) + "/" + mapReduce.getMemoryKey());
                FileOutputFormat.setOutputPath(reduceSortJob, sortedMemoryPath);
                reduceSortJob.waitForCompletion(true);
                FileSystem.get(newConfiguration).delete(memoryPath, true); // delete the temporary memory path
                memoryPath = sortedMemoryPath;
            }

            // if its not a SequenceFile there is no certain way to convert to necessary Java objects.
            // to get results you have to look through HDFS directory structure. Oh the horror.
            if (newConfiguration.getClass(Constants.GREMLIN_GIRAPH_MEMORY_OUTPUT_FORMAT_CLASS, SequenceFileOutputFormat.class, OutputFormat.class).equals(SequenceFileOutputFormat.class))
                mapReduce.addResultToMemory(memory, new KryoWritableIterator(configuration, memoryPath));
            else
                GiraphGraphComputer.LOGGER.warn(SEQUENCE_WARNING);
        }
    }

    public static <MK, MV, RK, RV, R> MapReduce<MK, MV, RK, RV, R> getMapReduce(final Configuration configuration) {
        try {
            final Class<? extends MapReduce> mapReduceClass = configuration.getClass(Constants.GRELMIN_GIRAPH_MAP_REDUCE_CLASS, MapReduce.class, MapReduce.class);
            final Constructor<? extends MapReduce> constructor = mapReduceClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            final MapReduce<MK, MV, RK, RV, R> mapReduce = constructor.newInstance();
            mapReduce.loadState(ConfUtil.makeApacheConfiguration(configuration));
            return mapReduce;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
