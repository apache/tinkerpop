package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.Traversal;
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

   /* @Test
    public void test() throws Exception {
        Graph g = TinkerGraph.open();
        GraphMLReader reader = GraphMLReader.create().build();
        reader.readGraph(new FileInputStream("/Users/marko/software/tinkerpop/tinkerpop3/data/grateful-dead.xml"), g);

        GraphSONWriter writer = GraphSONWriter.create().build();
        g.V().forEach(v -> {
            try {
                final FileOutputStream fos = new FileOutputStream("/tmp/grateful-dead-adjlist.json", true);
                fos.write("\n".getBytes());
                writer.writeVertex(fos, v, Direction.BOTH);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });
    } */
}
