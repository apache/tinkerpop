package org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolsConfigurable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;

import java.io.IOException;

/**
 * Created by Edi Bice on 6/18/2015.
 */
public final class GraphSONLegacyInputFormat extends FileInputFormat<NullWritable, VertexWritable> implements HadoopPoolsConfigurable {

    @Override
    public RecordReader<NullWritable, VertexWritable> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader<NullWritable, VertexWritable> reader = new GraphSONLegacyRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }
}
