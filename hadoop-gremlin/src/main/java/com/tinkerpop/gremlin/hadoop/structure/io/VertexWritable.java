package com.tinkerpop.gremlin.hadoop.structure.io;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexWritable<V extends Vertex> implements Writable {

    private Vertex vertex;
    private static final KryoWriter KRYO_WRITER = KryoWriter.build().create();
    private static final KryoReader KRYO_READER = KryoReader.build().create();

    public VertexWritable(final Vertex vertex) {
        this.vertex = vertex;
    }

    public void set(final Vertex vertex) {
        this.vertex = vertex;
    }

    public Vertex get() {
        return this.vertex;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[WritableUtils.readVInt(input)]);
        final Graph gLocal = TinkerGraph.open();
        this.vertex = KRYO_READER.readVertex(inputStream, Direction.BOTH,
                detachedVertex -> DetachedVertex.addTo(gLocal, detachedVertex),
                detachedEdge -> DetachedEdge.addTo(gLocal, detachedEdge));

    }

    @Override
    public void write(final DataOutput output) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        KRYO_WRITER.writeVertex(outputStream, this.vertex, Direction.BOTH);
        WritableUtils.writeVInt(output, outputStream.size());
        output.write(outputStream.toByteArray());
        outputStream.close();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof VertexWritable && ElementHelper.areEqual(this.vertex, ((VertexWritable) other).get());
    }

    @Override
    public int hashCode() {
        return this.vertex.hashCode();
    }

    @Override
    public String toString() {
        return this.vertex.toString();
    }
}
