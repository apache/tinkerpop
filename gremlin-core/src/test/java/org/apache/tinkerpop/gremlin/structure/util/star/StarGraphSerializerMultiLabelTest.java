/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedSerializerAdapter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

/**
 * Confirms that a multi-label {@link org.apache.tinkerpop.gremlin.structure.Vertex} round-trips through
 * {@link StarGraphSerializer}'s Kryo format without losing any of its labels, and that the {@code VERSION_1}
 * (single label per vertex) byte format written by prior versions of this serializer remains readable.
 */
public class StarGraphSerializerMultiLabelTest {

    /**
     * Replicates the pre-multi-label {@code StarGraphSerializer.write()} logic exactly (a single string label,
     * under the {@code VERSION_1} byte) so that this test can confirm the current reader still accepts data
     * written in that legacy shape, without depending on the current (fixed) writer to produce it.
     */
    private static final byte LEGACY_VERSION_1 = Byte.MIN_VALUE;

    @Test
    public void shouldRoundTripMultiLabelVertex() throws Exception {
        final StarGraph starGraph = StarGraph.open();
        final Vertex original = starGraph.addVertex(T.label, "animal");
        ((StarGraph.StarVertex) original).setLabels(new LinkedHashSet<>(Arrays.asList("animal", "bird", "aquatic", "endangered")));

        final GryoWriter writer = GryoWriter.build().create();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writer.writeVertex(baos, original, Direction.BOTH);

        final GryoReader reader = GryoReader.build().create();
        final Vertex roundTripped = reader.readVertex(new ByteArrayInputStream(baos.toByteArray()), Attachable::get);

        assertThat(roundTripped.labels(), is(new LinkedHashSet<>(Arrays.asList("animal", "bird", "aquatic", "endangered"))));
    }

    @Test
    public void shouldRoundTripSingleLabelVertex() throws Exception {
        final StarGraph starGraph = StarGraph.open();
        final Vertex original = starGraph.addVertex(T.label, "person");

        final GryoWriter writer = GryoWriter.build().create();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writer.writeVertex(baos, original, Direction.BOTH);

        final GryoReader reader = GryoReader.build().create();
        final Vertex roundTripped = reader.readVertex(new ByteArrayInputStream(baos.toByteArray()), Attachable::get);

        assertThat(roundTripped.label(), is("person"));
        assertThat(roundTripped.labels(), contains("person"));
    }

    @Test
    public void shouldReadLegacyVersion1SingleLabelFormat() throws Exception {
        final Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(StarGraph.class, new ShadedSerializerAdapter<>(new StarGraphSerializer(Direction.BOTH, new GraphFilter())));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        // hand-write the exact legacy (pre-multi-label) byte layout: version byte, null edge/meta property maps,
        // a class-and-object vertex id, then a single string label - mirroring the old write() implementation.
        output.writeByte(LEGACY_VERSION_1);
        kryo.writeObjectOrNull(output, null, HashMap.class); // edgeProperties
        kryo.writeObjectOrNull(output, null, HashMap.class); // metaProperties
        kryo.writeClassAndObject(output, 100); // vertex id
        kryo.writeObject(output, "person"); // single string label, as the old format wrote it
        kryo.writeObject(output, false); // no in-edges
        kryo.writeObject(output, false); // no out-edges
        kryo.writeObject(output, false); // no vertex properties
        output.flush();

        final Input input = new Input(baos.toByteArray());
        final StarGraph starGraph = kryo.readObject(input, StarGraph.class);

        assertThat(starGraph.getStarVertex().label(), is("person"));
        assertThat(starGraph.getStarVertex().labels(), contains("person"));
    }
}
