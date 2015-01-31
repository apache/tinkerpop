package com.tinkerpop.gremlin.hadoop.structure.io.script;

import com.tinkerpop.gremlin.hadoop.structure.io.CommonFileOutputFormat;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public class ScriptOutputFormat extends CommonFileOutputFormat {

    @Override
    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return getRecordWriter(job, getDataOuputStream(job));
    }

    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(final TaskAttemptContext job, final DataOutputStream outputStream) throws IOException, InterruptedException {
        return new ScriptRecordWriter(outputStream, job);
    }
}