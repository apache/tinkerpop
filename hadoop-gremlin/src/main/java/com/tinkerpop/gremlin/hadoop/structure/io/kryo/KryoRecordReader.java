package com.tinkerpop.gremlin.hadoop.structure.io.kryo;

import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.structure.io.kryo.KryoMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoRecordReader extends RecordReader<NullWritable, VertexWritable> {

    private VertexStreamIterator vertexStreamIterator;
    private FSDataInputStream inputStream;

    private static final byte[] PATTERN = KryoMapper.build().create().getVersionedHeader();

    public KryoRecordReader() {
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        final FileSplit split = (FileSplit) genericSplit;
        final Configuration job = context.getConfiguration();
        long start = split.getStart();
        final Path file = split.getPath();
        if (null != new CompressionCodecFactory(job).getCodec(file)) {
            throw new IllegalStateException("Compression is not supported for the (binary) Gremlin Kryo format");
        }
        // open the file and seek to the start of the split
        this.inputStream = file.getFileSystem(job).open(split.getPath());
        this.inputStream.seek(start);
        final long newStart = seekToHeader(this.inputStream, start);
        this.vertexStreamIterator = new VertexStreamIterator(this.inputStream, split.getLength() - (newStart - start));
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
        return this.vertexStreamIterator.hasNext();
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() {
        return this.vertexStreamIterator.next();
    }

    @Override
    public float getProgress() throws IOException {
        return this.vertexStreamIterator.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.inputStream.close();
    }
}
