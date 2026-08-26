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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The {@code io()} step reads bytes the caller may not control, so the Gryo reader and writer it builds must refuse
 * native Java serialization. Nothing else in this module exercises {@link IoStep#constructReader()} or
 * {@link IoStep#constructWriter()}, so without these tests that hardening could be dropped and the build would stay
 * green.
 */
public class IoStepTest {

    /**
     * The Gryo type id of {@code OptionsStrategy}, registered with the shaded {@code JavaSerializer} in both
     * {@code GryoVersion.V1_0} and {@code GryoVersion.V3_0}, pinned against the registry in
     * {@link #shouldInvokeJavaDeserializationOnAFullFidelityGryoReader()}.
     */
    private static final int OPTIONS_STRATEGY_GRYO_ID = 187;

    /**
     * Kryo shifts written class ids to leave room for its {@code NULL} and {@code NAME} markers.
     */
    private static final int CLASS_ID_OFFSET = 2;

    /**
     * Kryo's reference marker for an object being seen for the first time.
     */
    private static final int KRYO_NOT_NULL = 1;

    /**
     * {@code readObject} is used deliberately rather than the {@code readGraph} that {@code io().read()} calls.
     * {@code readGraph} checks the file header first, so the crafted stream would be rejected on its first byte and
     * the canary would never run. Both decode with the same mapper this step builds.
     */
    @Test
    public void shouldNotInvokeJavaDeserializationOnTheGryoReaderIoBuilds() throws Exception {
        final GraphReader reader = new IoStep<>(__.start().asAdmin(), "graph.kryo").constructReader();
        // without this, a dispatch regression that returned some other reader would still pass below, since the
        // crafted Gryo bytes would simply be refused and the canary would stay unset
        assertThat(reader, instanceOf(GryoReader.class));

        DeserializationCanary.FIRED = false;
        try (final InputStream stream = new ByteArrayInputStream(maliciousGryoBytes())) {
            reader.readObject(stream, Object.class);
        } catch (Exception ignored) {
            // refusing the stream outright is the expected outcome, since the JavaSerializer backed registration was
            // dropped. what matters is that nothing was deserialized on the way to that decision
        }

        assertFalse("the reader io() builds must not invoke ObjectInputStream.readObject() on the bytes it reads",
                DeserializationCanary.FIRED);
    }

    /**
     * Positive control for the test above. The same crafted stream must reach {@code ObjectInputStream.readObject()}
     * through a full fidelity reader, otherwise that assertion could hold for the wrong reason and prove nothing.
     */
    @Test
    public void shouldInvokeJavaDeserializationOnAFullFidelityGryoReader() throws Exception {
        final GryoMapper fullFidelity = GryoMapper.build().create();
        assertEquals("OptionsStrategy's gryo id changed, so the crafted stream no longer selects the JavaSerializer",
                OPTIONS_STRATEGY_GRYO_ID,
                fullFidelity.createMapper().getRegistration(OptionsStrategy.class).getId());

        final GryoReader reader = GryoReader.build().mapper(fullFidelity).create();

        DeserializationCanary.FIRED = false;
        try (final InputStream stream = new ByteArrayInputStream(maliciousGryoBytes())) {
            reader.readObject(stream, Object.class);
        } catch (Exception ignored) {
            // the payload deserializes to the canary rather than to an OptionsStrategy, so a failure is possible
            // here, but it would come after readObject() has already run
        }

        assertTrue("the crafted stream must reach ObjectInputStream.readObject() on a full fidelity reader, " +
                        "otherwise shouldNotInvokeJavaDeserializationOnTheGryoReaderIoBuilds proves nothing",
                DeserializationCanary.FIRED);
    }

    /**
     * The writer is hardened on the same terms as the reader, so that {@code io()} cannot write a document it will
     * not read back.
     */
    @Test
    public void shouldNotWriteTypesWithJavaSerializerOnTheGryoWriterIoBuilds() throws Exception {
        final GraphWriter writer = new IoStep<>(__.start().asAdmin(), "graph.kryo").constructWriter();

        try (final OutputStream stream = new ByteArrayOutputStream()) {
            writer.writeObject(stream, OptionsStrategy.build().with("some-key", "some-value").create());
            fail("the writer io() builds must not write a JavaSerializer backed type");
        } catch (IllegalArgumentException expected) {
            // Kryo refuses the unregistered class, since the registration was dropped
        }
    }

    /**
     * {@code io()} takes a class name for its reader, writer and registry, and the traversal that supplies it may have
     * arrived as a remote request, so a name that is not the type the parameter asks for has to be refused before the
     * class it names is initialized. The canary classes below record any of their code running in {@link IoCanary},
     * which is a separate class so that reading the flag does not initialize the canary being watched.
     */
    @Test
    public void shouldNotInitializeANamedReaderThatIsNotAGraphReader() {
        IoCanary.FIRED = false;

        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), "graph.kryo");
        // a class literal does not initialize the class it names, so naming the canary this way does not fire it
        step.configure(IO.reader, NotAnIoType.class.getName());

        try {
            step.constructReader();
            fail("a class that is not a GraphReader must not be accepted as one");
        } catch (IllegalStateException expected) {
            // the name is refused, and the assertion below is what makes the refusal meaningful
        }

        assertFalse("a class named by IO.reader must not be initialized before it is known to be a GraphReader",
                IoCanary.FIRED);
    }

    @Test
    public void shouldNotInitializeANamedWriterThatIsNotAGraphWriter() {
        IoCanary.FIRED = false;

        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), "graph.kryo");
        step.configure(IO.writer, NotAnIoType.class.getName());

        try {
            step.constructWriter();
            fail("a class that is not a GraphWriter must not be accepted as one");
        } catch (IllegalStateException expected) {
            // as above
        }

        assertFalse("a class named by IO.writer must not be initialized before it is known to be a GraphWriter",
                IoCanary.FIRED);
    }

    @Test
    public void shouldNotInitializeANamedRegistryThatIsNotAnIoRegistry() {
        IoCanary.FIRED = false;

        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), "graph.kryo");
        step.configure(IO.registry, NotAnIoType.class.getName());

        try {
            step.detectRegistries();
            fail("a class that is not an IoRegistry must not be accepted as one");
        } catch (IllegalStateException expected) {
            // as above
        }

        assertFalse("a class named by IO.registry must not be initialized before it is known to be an IoRegistry",
                IoCanary.FIRED);
    }

    /**
     * The named class is an {@link IoRegistry} here, so it passes the type check and the factory method is what has to
     * be rejected. {@code instance()} is called with a {@code null} receiver, so a non-static one was never going to
     * work, but checking the signature rather than discovering it through the call is what keeps the class from being
     * initialized on the way to the error.
     */
    @Test
    public void shouldNotInitializeANamedRegistryWhoseInstanceMethodIsNotStatic() {
        IoCanary.FIRED = false;

        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), "graph.kryo");
        step.configure(IO.registry, NonStaticInstanceRegistry.class.getName());

        try {
            step.detectRegistries();
            fail("a non-static instance() must not be accepted as the factory for an IoRegistry");
        } catch (IllegalStateException expected) {
            // as above
        }

        assertFalse("a class whose instance() is not static must not be initialized on the way to that error",
                IoCanary.FIRED);
    }

    /**
     * The documented form of these parameters, which GLVs have no alternative to since they cannot send an instance
     * over the wire. The hardening above narrows what a name may resolve to and must not withdraw the feature.
     */
    @Test
    public void shouldConstructAGraphReaderNamedByClassName() {
        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), "graph.kryo");
        step.configure(IO.reader, GryoReader.class.getName());

        assertThat(step.constructReader(), instanceOf(GryoReader.class));
    }

    @Test
    public void shouldConstructAGraphWriterNamedByClassName() {
        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), "graph.kryo");
        step.configure(IO.writer, GryoWriter.class.getName());

        assertThat(step.constructWriter(), instanceOf(GryoWriter.class));
    }

    @Test
    public void shouldConstructAnIoRegistryNamedByClassName() {
        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), "graph.kryo");
        step.configure(IO.registry, StaticInstanceRegistry.class.getName());

        final List<IoRegistry> registries = step.detectRegistries();
        assertEquals(1, registries.size());
        assertThat(registries.get(0), instanceOf(StaticInstanceRegistry.class));
    }

    /**
     * Opening the output stream creates or truncates the destination, so it must not be opened until the writer is in
     * hand. Otherwise naming a writer that cannot be constructed destroys the contents of whatever file the traversal
     * pointed at, and the traversal chooses that path.
     */
    @Test
    public void shouldNotTruncateTheTargetFileWhenTheWriterCannotBeConstructed() throws Exception {
        final File target = File.createTempFile("io-step-target", ".kryo");
        target.deleteOnExit();
        final byte[] contents = "do not truncate me".getBytes();
        Files.write(target.toPath(), contents);

        final IoStep<?> step = new IoStep<>(__.start().asAdmin(), target.getAbsolutePath());
        step.configure(IO.writer, NotAnIoType.class.getName());

        try {
            step.write(target);
            fail("a writer that cannot be constructed must fail the write");
        } catch (IllegalStateException expected) {
            // the writer is resolved first, so the failure arrives before the file is opened
        }

        assertArrayEquals("a writer that cannot be constructed must leave the target file untouched",
                contents, Files.readAllBytes(target.toPath()));
    }

    /**
     * A Gryo stream that presents {@code OptionsStrategy}'s type id and then a raw Java-serialized payload. Crafting
     * it needs no cooperation from the Gryo writer, which is why the sink was reachable from untrusted bytes.
     */
    private byte[] maliciousGryoBytes() throws Exception {
        final ByteArrayOutputStream javaPayload = new ByteArrayOutputStream();
        try (final ObjectOutputStream oos = new ObjectOutputStream(javaPayload)) {
            oos.writeObject(new DeserializationCanary());
        }

        final Output malicious = new Output(javaPayload.size() + 64, -1);
        malicious.writeVarInt(OPTIONS_STRATEGY_GRYO_ID + CLASS_ID_OFFSET, true);
        malicious.writeVarInt(KRYO_NOT_NULL, true);
        malicious.writeBytes(javaPayload.toByteArray());
        malicious.flush();
        return malicious.toBytes();
    }

    /**
     * A deliberately inert {@code Serializable} used to detect whether native Java deserialization ran during a Gryo
     * read. It touches nothing outside this class: no process execution, no filesystem, no reflection.
     */
    private static class DeserializationCanary implements Serializable {
        private static final long serialVersionUID = 1L;

        static volatile boolean FIRED = false;

        private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            FIRED = true;
        }
    }

    /**
     * Holds the flag the canary classes below set. It is deliberately not a field on those classes, since reading a
     * static field initializes the class that declares it, which is the very thing the tests assert did not happen.
     */
    public static class IoCanary {
        static volatile boolean FIRED = false;
    }

    /**
     * Not a {@link GraphReader}, a {@link GraphWriter} or an {@link IoRegistry}, and it reports every route by which
     * {@code io()} might run its code: its static initializer, and the two factory method names the step looks for.
     * The bodies are inert, since firing the flag is all that has to be observable.
     */
    public static class NotAnIoType {
        static {
            IoCanary.FIRED = true;
        }

        public static Object build() {
            IoCanary.FIRED = true;
            return null;
        }

        public static Object instance() {
            IoCanary.FIRED = true;
            return null;
        }
    }

    /**
     * An {@link IoRegistry}, so it clears the type check, whose {@code instance()} is not static.
     */
    public static class NonStaticInstanceRegistry extends AbstractIoRegistry {
        static {
            IoCanary.FIRED = true;
        }

        public IoRegistry instance() {
            return this;
        }
    }

    /**
     * A well-formed registry, used to hold the documented class-name form open.
     */
    public static class StaticInstanceRegistry extends AbstractIoRegistry {
        private static final StaticInstanceRegistry INSTANCE = new StaticInstanceRegistry();

        public static StaticInstanceRegistry instance() {
            return INSTANCE;
        }
    }
}
