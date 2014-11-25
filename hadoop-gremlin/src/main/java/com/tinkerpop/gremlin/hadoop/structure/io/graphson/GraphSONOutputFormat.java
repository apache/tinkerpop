package com.tinkerpop.gremlin.hadoop.structure.io.graphson;

import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.hadoop.structure.io.CommonFileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONOutputFormat extends CommonFileOutputFormat {

    @Override
    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return new GraphSONRecordWriter(getDataOuputStream(job));
    }

    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(final TaskAttemptContext job,
                                                              final DataOutputStream outputStream) throws IOException, InterruptedException {
        return new GraphSONRecordWriter(outputStream);
    }
}