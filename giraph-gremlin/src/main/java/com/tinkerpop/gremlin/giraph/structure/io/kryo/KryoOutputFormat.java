package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.structure.io.CommonOutputFormat;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class KryoOutputFormat extends CommonOutputFormat {

    @Override
    public RecordWriter<NullWritable, GiraphInternalVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return new KryoRecordWriter(getDataOuputStream(job));
    }

    public RecordWriter<NullWritable, GiraphInternalVertex> getRecordWriter(final TaskAttemptContext job,
                                                                            final DataOutputStream outputStream) throws IOException, InterruptedException {
        return new KryoRecordWriter(outputStream);
    }
}