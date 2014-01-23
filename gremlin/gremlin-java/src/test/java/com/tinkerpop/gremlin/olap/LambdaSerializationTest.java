package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.HolderIterator;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaSerializationTest {

    @Test
    public void shouldSerializeLambda() throws Exception {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream(10);
        ObjectOutputStream out = new ObjectOutputStream(outBytes);
        final SerializedPredicate predicate = e -> true;
        out.writeObject(predicate);

        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(outBytes.toByteArray()));
        Predicate p = (Predicate) in.readObject();
        assertTrue(p.test(null));
    }

    @Test
    public void shouldSerializeGremlinSupplier() throws Exception {
        Graph g = TinkerFactory.createClassic();
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream(10);
        ObjectOutputStream out = new ObjectOutputStream(outBytes);
        final SerializedSupplier<Pipeline> supplier = () -> Gremlin.of().out().filter(h -> true);
        out.writeObject(supplier);

        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(outBytes.toByteArray()));
        Pipeline pipeline = ((Supplier<Pipeline>) in.readObject()).get();
        pipeline.addStarts(new HolderIterator<>(g.query().vertices().iterator()));
        pipeline.forEach(System.out::println);
    }

    public interface SerializedPredicate<T> extends Predicate<T>, Serializable {
    }

    public interface SerializedSupplier<T> extends Supplier<T>, Serializable {
    }
}
