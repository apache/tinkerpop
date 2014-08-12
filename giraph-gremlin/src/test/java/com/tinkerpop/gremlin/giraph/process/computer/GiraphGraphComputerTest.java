package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalMessage;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.util.Serializer;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputerTest {

    public static class TraversalHolder implements SSupplier<Traversal> {
        public Traversal get() {
            return TinkerGraph.open().V().out().filter(v -> true);
        }
    }

    @Test
    public void testTraversalSupplierSerialization() throws Exception {
        final File tempFile = File.createTempFile("traversalSupplier-" + UUID.randomUUID(), ".bin");

        byte[] bytes = Serializer.serializeObject(new TraversalHolder());
        FileOutputStream outputStream = new FileOutputStream(tempFile);
        outputStream.write(bytes);
        outputStream.flush();
        outputStream.close();

        byte[] bytes2 = new byte[(int) tempFile.length()];
        FileInputStream inputStream = new FileInputStream(tempFile);
        new DataInputStream(inputStream).readFully(bytes2);
        tempFile.delete();
    }

    @Test
    public void shouldDoKryoCorrectly() throws Exception {
        KryoWritable<TraversalMessage> writable = new KryoWritable<>();
        Graph g = TinkerFactory.createClassic();
        Traverser traverser = new SimpleTraverser<>(DetachedVertex.detach(g.v(1)));
        TraversalMessage message = TraversalMessage.of(traverser);
        writable.set(message);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(outputStream);
        writable.write(out);
        outputStream.flush();
        outputStream.close();
        final DataInput in = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        writable.readFields(in);

        /*KryoWritable<TraversalMessage> writable2 = new KryoWritable<>();
        final File tempFile = new File("/tmp/test.bin");
        final FileOutputStream outputStream = new FileOutputStream(tempFile);
        final DataOutput out = new DataOutputStream(outputStream);
        writable.write(out);
        outputStream.flush();
        outputStream.close();
        final DataInput in = new DataInputStream(new FileInputStream(tempFile));
        writable2.readFields(in);
        TraversalMessage message2 = writable2.get();
        System.out.println(message2.getTraverser().get().getClass());*/
    }
}
