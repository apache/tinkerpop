package com.tinkerpop.gremlin.structure;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Serialization tests that occur at a lower level than IO.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class SerializationTest {
    public static class KryoTest extends AbstractGremlinTest {
        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexAsDetached() throws Exception {
            final GremlinKryo gremlinKryo = g.io().gremlinKryoSerializer();
            final Kryo kryo = gremlinKryo.createKryo();
            final Vertex v = g.V(convertToVertexId("marko")).next();
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final Output output = new Output(stream);
            kryo.writeObject(output, v);
            output.close();

            final Input input = new Input(stream.toByteArray());
            final Vertex detached = kryo.readObject(input, Vertex.class);
            input.close();
            assertNotNull(detached);
            assertEquals(v.label(), detached.label());
            assertEquals(v.id(), detached.id());
            assertEquals(v.value("name").toString(), detached.value("name"));
            assertEquals((Integer) v.value("age"), detached.value("age"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeEdgeAsDetached() {
            final GremlinKryo gremlinKryo = g.io().gremlinKryoSerializer();
            final Kryo kryo = gremlinKryo.createKryo();
            final Edge e = g.E(convertToEdgeId("marko", "knows", "vadas")).next();
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final Output output = new Output(stream);
            kryo.writeObject(output, e);
            output.close();

            final Input input = new Input(stream.toByteArray());
            final Edge detached = kryo.readObject(input, Edge.class);
            assertNotNull(detached);
            assertEquals(e.label(), detached.label());
            assertEquals(e.id(), detached.id());
            assertEquals((Double) e.value("weight"), detached.value("weight"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePropertyAsDetached() {
            final GremlinKryo gremlinKryo = g.io().gremlinKryoSerializer();
            final Kryo kryo = gremlinKryo.createKryo();
            final Property p = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final Output output = new Output(stream);
            kryo.writeObject(output, p);
            output.close();

            final Input input = new Input(stream.toByteArray());
            final Property detached = kryo.readObject(input, Property.class);
            assertNotNull(detached);
            assertEquals(p.key(), detached.key());
            assertEquals(p.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexPropertyAsDetached() throws Exception {
            final GremlinKryo gremlinKryo = g.io().gremlinKryoSerializer();
            final Kryo kryo = gremlinKryo.createKryo();
            final VertexProperty vp = g.V(convertToVertexId("marko")).next().property("name");
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final Output output = new Output(stream);
            kryo.writeObject(output, vp);
            output.close();

            final Input input = new Input(stream.toByteArray());
            final VertexProperty detached = kryo.readObject(input, VertexProperty.class);
            input.close();
            assertNotNull(detached);
            assertEquals(vp.label(), detached.label());
            assertEquals(vp.id(), detached.id());
            assertEquals(vp.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.CREW)
        public void shouldSerializeVertexPropertyWithPropertiesAsDetached() throws Exception {
            final GremlinKryo gremlinKryo = g.io().gremlinKryoSerializer();
            final Kryo kryo = gremlinKryo.createKryo();
            final VertexProperty vp = g.V(convertToVertexId("marko")).next().iterators().propertyIterator("location").next();
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final Output output = new Output(stream);
            kryo.writeObject(output, vp);
            output.close();

            final Input input = new Input(stream.toByteArray());
            final VertexProperty detached = kryo.readObject(input, VertexProperty.class);
            input.close();
            assertNotNull(detached);
            assertEquals(vp.label(), detached.label());
            assertEquals(vp.id(), detached.id());
            assertEquals(vp.value(), detached.value());
            assertEquals(vp.iterators().propertyIterator("startTime").next().value(), detached.iterators().propertyIterator("startTime").next().value());
            assertEquals(vp.iterators().propertyIterator("startTime").next().key(), detached.iterators().propertyIterator("startTime").next().key());
            assertEquals(vp.iterators().propertyIterator("endTime").next().value(), detached.iterators().propertyIterator("endTime").next().value());
            assertEquals(vp.iterators().propertyIterator("endTime").next().key(), detached.iterators().propertyIterator("endTime").next().key());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePathAsDetached() throws Exception {
            final GremlinKryo gremlinKryo = g.io().gremlinKryoSerializer();
            final Kryo kryo = gremlinKryo.createKryo();
            final Path p = g.V(convertToVertexId("marko")).as("a").outE().as("b").inV().as("c").path().next();
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final Output output = new Output(stream);
            kryo.writeObject(output, p);
            output.close();

            final Input input = new Input(stream.toByteArray());
            final Path detached = kryo.readObject(input, Path.class);
            input.close();
            assertNotNull(detached);
            assertEquals(p.labels().size(), detached.labels().size());
            assertEquals(p.labels().get(0).size(), detached.labels().get(0).size());
            assertEquals(p.labels().get(1).size(), detached.labels().get(1).size());
            assertEquals(p.labels().get(2).size(), detached.labels().get(2).size());
            assertTrue(p.labels().stream().flatMap(Collection::stream).allMatch(detached::hasLabel));

            final Vertex vOut = p.get("a");
            final Vertex detachedVOut = detached.get("a");
            assertEquals(vOut.label(), detachedVOut.label());
            assertEquals(vOut.id(), detachedVOut.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVOut.iterators().propertyIterator().hasNext());

            final Edge e = p.get("b");
            final Edge detachedE = detached.get("b");
            assertEquals(e.label(), detachedE.label());
            assertEquals(e.id(), detachedE.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedE.iterators().propertyIterator().hasNext());

            final Vertex vIn = p.get("c");
            final Vertex detachedVIn = detached.get("c");
            assertEquals(vIn.label(), detachedVIn.label());
            assertEquals(vIn.id(), detachedVIn.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVIn.iterators().propertyIterator().hasNext());

        }
    }
}
