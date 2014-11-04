package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class KryoRecordReader extends RecordReader<NullWritable, GiraphComputeVertex> {

    private VertexStreamIterator vertexStreamIterator;
    private FSDataInputStream inputStream;

    private float progress = 0f;

    public KryoRecordReader() {
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        long start = split.getStart();
        //long end = start + split.getLength();
        final Path file = split.getPath();
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        if (null != compressionCodecs.getCodec(file)) {
            throw new IllegalStateException("compression is not supported for the (binary) Gremlin Kryo format");
        }

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        inputStream = fs.open(split.getPath());
        inputStream.seek(start);
        vertexStreamIterator = new VertexStreamIterator(inputStream, KryoReader.build().create());
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        boolean hasNext = vertexStreamIterator.hasNext();

        // this is a pretty coarse metric of progress, as we don't have a reliable way to estimate the number of vertices in the chunk
        if (hasNext) {
            progress = 0.5f;
        } else if (progress > 0) {
            progress = 1f;
        }

        return hasNext;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public GiraphComputeVertex getCurrentValue() {
        return new GiraphComputeVertex((TinkerVertex) vertexStreamIterator.next());
    }

    @Override
    public float getProgress() throws IOException {
        return progress;
    }

    @Override
    public synchronized void close() throws IOException {
        inputStream.close();
    }
}
