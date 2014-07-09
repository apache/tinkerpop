package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.util.Serializer;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunnerTest {

    public static class TraversalHolder implements SSupplier<Traversal> {
        public Traversal get() {
            return TinkerGraph.open().V().out().filter(v -> true);
        }
    }

    @Test
    public void testTraversalSupplierSerialization() throws Exception {

        final File tempFile = File.createTempFile("traversalSupplier-" + UUID.randomUUID(), ".bin");
        //System.out.println(tempFile.getPath());

        byte[] bytes = Serializer.serializeObject(new TraversalHolder());
        FileOutputStream outputStream = new FileOutputStream(tempFile);
        outputStream.write(bytes);
        outputStream.flush();
        outputStream.close();

        byte[] bytes2 = new byte[(int) tempFile.length()];
        FileInputStream inputStream = new FileInputStream(tempFile);
        new DataInputStream(inputStream).readFully(bytes2);
        System.out.println(((Supplier) Serializer.deserializeObject(bytes2)).get());

        tempFile.delete();
    }
}
