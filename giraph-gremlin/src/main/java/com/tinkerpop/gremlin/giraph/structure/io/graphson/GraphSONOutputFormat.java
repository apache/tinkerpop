package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.giraph.structure.io.CommonOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONOutputFormat extends CommonOutputFormat {

    @Override
    public RecordWriter<NullWritable, GiraphVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return new GraphSONRecordWriter(getDataOuputStream(job));
    }

    public RecordWriter<NullWritable, GiraphVertex> getRecordWriter(final TaskAttemptContext job,
                                                                    final DataOutputStream outputStream) throws IOException, InterruptedException {
        return new GraphSONRecordWriter(outputStream);
    }
}