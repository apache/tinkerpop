package com.tinkerpop.gremlin.hadoop.structure.io.memory;

import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StringOutputFormat extends FileOutputFormat<ObjectWritable, ObjectWritable> {

    private static final String TAB = "\t";

    @Override
    public RecordWriter<ObjectWritable, ObjectWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final DataOutputStream outputStream = getDataOuputStream(taskAttemptContext);
        return new RecordWriter<ObjectWritable, ObjectWritable>() {
            @Override
            public void write(final ObjectWritable key, final ObjectWritable value) throws IOException, InterruptedException {
                if (key.get() instanceof MapReduce.NullObject && value.get() instanceof MapReduce.NullObject) {
                    return;
                } else if (key.get() instanceof MapReduce.NullObject) {
                    outputStream.writeBytes(value.get().toString());
                } else if (value.get() instanceof MapReduce.NullObject) {
                    outputStream.writeBytes(key.get().toString());
                } else {
                    outputStream.writeBytes(key.get().toString() + TAB + value.get().toString());
                }
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                outputStream.close();
            }
        };
    }

    protected DataOutputStream getDataOuputStream(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path file = super.getDefaultWorkFile(job, extension);
        final FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            return new DataOutputStream(fs.create(file, false));
        } else {
            return new DataOutputStream(codec.createOutputStream(fs.create(file, false)));
        }
    }


}
