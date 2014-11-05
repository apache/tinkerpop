package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoRecordReader extends RecordReader<NullWritable, GiraphComputeVertex> {

    private VertexStreamIterator vertexStreamIterator;
    private FSDataInputStream inputStream;

    private float progress = 0f;
    private static final byte[] PATTERN = GremlinKryo.build().create().getVersionedHeader();

    public KryoRecordReader() {
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        long start = split.getStart();
        final Path file = split.getPath();
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        if (null != compressionCodecs.getCodec(file)) {
            throw new IllegalStateException("Compression is not supported for the (binary) Gremlin Kryo format");
        }
        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        inputStream = fs.open(split.getPath());
        inputStream.seek(start);
        final long newStart = seekToHeader(inputStream, start);
        vertexStreamIterator = new VertexStreamIterator(inputStream, split.getLength() - (newStart - start), KryoReader.build().create());
    }

    private static long seekToHeader(final FSDataInputStream inputStream, final long start) throws IOException {
        long nextStart = start;
        final byte[] buffer = new byte[32];
        while (true) {
            if ((buffer[0] = PATTERN[0]) == inputStream.readByte()) {
                inputStream.read(nextStart + 1, buffer, 1, 31);
                if (patternMatch(buffer)) {
                    inputStream.seek(nextStart);
                    return nextStart;
                }
            } else {
                nextStart = nextStart + 1;
                inputStream.seek(nextStart);
            }
        }
    }

    private static boolean patternMatch(final byte[] bytes) {
        for (int i = 0; i < 31; i++) {
            if (bytes[i] != PATTERN[i])
                return false;
        }
        return true;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        boolean hasNext = vertexStreamIterator.hasNext();

        // this is a pretty coarse metric of progress, as we don't have a reliable way to estimate the number of vertices in the chunk
        // TODO: vertexStreamIterator can provide access to its currentLength variable
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
        return new GiraphComputeVertex((TinkerVertex) vertexStreamIterator.next()); // TODO: this is hardcoded for TinkerVertex
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
