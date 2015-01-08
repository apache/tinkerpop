package com.tinkerpop.gremlin.structure;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
    }
}
