/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.VertexTerminator;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoRecordReader extends RecordReader<NullWritable, VertexWritable> {

    private FSDataInputStream inputStream;

    private static final byte[] PATTERN = GryoMapper.HEADER;
    private static final byte[] TERMINATOR = VertexTerminator.instance().terminal;

    private final GryoReader gryoReader = GryoReader.build().create();
    private final VertexWritable vertexWritable = new VertexWritable();
    private boolean hasEdges;

    private long currentLength = 0;
    private long splitLength;

    public GryoRecordReader() {
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        final FileSplit split = (FileSplit) genericSplit;
        final Configuration job = context.getConfiguration();
        long start = split.getStart();
        final Path file = split.getPath();
        if (null != new CompressionCodecFactory(job).getCodec(file)) {
            throw new IllegalStateException("Compression is not supported for the (binary) Gryo format");
        }
        // open the file and seek to the start of the split
        this.inputStream = file.getFileSystem(job).open(split.getPath());
        this.splitLength = split.getLength() - (seekToHeader(this.inputStream, start) - start);
        this.hasEdges = context.getConfiguration().getBoolean(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT_HAS_EDGES, true);
    }

    private static long seekToHeader(final FSDataInputStream inputStream, final long start) throws IOException {
        inputStream.seek(start);
        long nextStart = start;
        final byte[] buffer = new byte[PATTERN.length];
        while (true) {
            if ((buffer[0] = PATTERN[0]) == inputStream.readByte()) {
                inputStream.read(nextStart + 1, buffer, 1, PATTERN.length - 1);
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
        for (int i = 0; i < PATTERN.length - 1; i++) {
            if (bytes[i] != PATTERN[i])
                return false;
        }
        return true;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (this.currentLength >= this.splitLength)
            return false;

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        long currentVertexLength = 0;
        int terminatorLocation = 0;
        while (true) {
            final int currentByte = this.inputStream.read();
            if (-1 == currentByte) {
                if (currentVertexLength > 0)
                    throw new IllegalStateException("Remainder of stream exhausted without matching a vertex");
                else
                    return false;
            }
            this.currentLength++;
            currentVertexLength++;
            output.write(currentByte);

            terminatorLocation = ((byte) currentByte) == TERMINATOR[terminatorLocation] ? terminatorLocation + 1 : 0;
            if (terminatorLocation >= TERMINATOR.length) {
                final StarGraph starGraph = StarGraph.open();
                final Function<Attachable<Vertex>, Vertex> vertexMaker = attachableVertex -> attachableVertex.attach(Attachable.Method.create(starGraph));
                final Function<Attachable<Edge>, Edge> edgeMaker = attachableEdge -> attachableEdge.attach(Attachable.Method.create(starGraph));
                try (InputStream in = new ByteArrayInputStream(output.toByteArray())) {
                    this.vertexWritable.set(this.hasEdges ?
                            this.gryoReader.readVertex(in, vertexMaker, edgeMaker, Direction.BOTH) :
                            this.gryoReader.readVertex(in, vertexMaker));
                    return true;
                }
            }
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() {
        return this.vertexWritable;
    }

    @Override
    public float getProgress() throws IOException {
        return (0 == this.currentLength || 0 == this.splitLength) ? 0.0f : (float) this.currentLength / (float) this.splitLength;
    }

    @Override
    public synchronized void close() throws IOException {
        this.inputStream.close();
    }
}
