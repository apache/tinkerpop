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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.step.sideEffect;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapClassLoadingPolicy;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapConfigKeyPolicy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HadoopIoStepTest {

    private static HadoopIoStep readStepWith(final String key, final Object value) {
        final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), "graph.kryo");
        step.setMode(ReadWriting.Mode.READING);
        step.configure(key, value);
        return step;
    }

    private static HadoopIoStep writeStepWith(final String key, final Object value) {
        final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), "output");
        step.setMode(ReadWriting.Mode.WRITING);
        step.configure(key, value);
        return step;
    }

    // A raw graphReader key set via io().with() is denied by the default config-key policy: it is neither a built-in
    // nor an operator-approved key. There is no class-value-specific gate here and the value never reaches class
    // loading -- this simply pins that the internal graphReader key stays out of the default allow-list. (The reader
    // FQCN supplied via the IO.reader token is gated separately; see shouldRejectReaderClassNameFromUntrustedTraversal.)
    @Test
    public void shouldDenyGraphReaderKeyByDefaultWhenUntrusted() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(Constants.GREMLIN_HADOOP_GRAPH_READER, "evil.Reader");
        try {
            step.generateProgram(graph, null);
            fail("a raw graphReader key from a remote traversal must be denied by default in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    // graphFilter is denied by the same default config-key policy because it is not a built-in/approved key. This is
    // NOT a graphFilter-deserialization guard: the value is never deserialized here, it is rejected at the key gate
    // before use (deserialization hardening of graphFilter is a separate concern). The test pins that graphFilter
    // stays out of the default allow-list, so an untrusted traversal cannot inject one.
    @Test
    public void shouldDenyGraphFilterKeyByDefaultWhenUntrusted() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(Constants.GREMLIN_HADOOP_GRAPH_FILTER, "rO0ABXNy"); // never deserialized -- rejected at the key gate
        try {
            step.generateProgram(graph, null);
            fail("graphFilter is not an approved io() key, so it must be denied for an untrusted traversal");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldRejectSelfElevationFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(OlapClassLoadingPolicy.TRUSTED, true);
        try {
            step.generateProgram(graph, null);
            fail("a remote traversal must not be able to set the IO trust flag on itself");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldAllowClassValuedOptionInTrustedConfiguration() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith(Constants.GREMLIN_HADOOP_GRAPH_READER, "my.custom.Reader");
        step.generateProgram(graph, null); // must not throw in trusted mode
        assertEquals("my.custom.Reader", graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    @Test
    public void shouldAcceptAnyReaderClassNameWhenTrusted() {
        // a trusted deployment restores the previous behavior: an arbitrary reader FQCN via IO.reader is accepted
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith(IO.reader, "my.custom.Reader");
        step.generateProgram(graph, null); // must not throw in trusted mode
        assertEquals("my.custom.Reader", graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    @Test
    public void shouldAcceptAnyWriterClassNameWhenTrusted() {
        // a trusted deployment restores the previous behavior: an arbitrary writer FQCN via IO.writer is accepted
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = writeStepWith(IO.writer, "my.custom.Writer");
        step.generateProgram(graph, null); // must not throw in trusted mode
        assertEquals("my.custom.Writer", graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
    }

    @Test
    public void shouldRejectUnapprovedOptionFromUntrustedTraversal() {
        // untrusted io().with() permits only operator-approved keys; an unapproved key is rejected
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith("spark.executor.memory", "2g");
        try {
            step.generateProgram(graph, null);
            fail("a remote traversal must not set an unapproved OLAP configuration option in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldAcceptApprovedGraphConfigKeyFromUntrustedTraversal() {
        // io().with() permits keys the operator approved via its own list (separate from the computer list)
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS, "my.graph.option");
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith("my.graph.option", "v");
        step.generateProgram(graph, null); // approved for the io() surface -> copied
        assertEquals("v", graph.configuration().getString("my.graph.option"));
    }

    @Test
    public void shouldRejectApprovedGraphConfigKeysMetaKeyFromUntrustedTraversal() {
        // the approved-key list itself is a trust-boundary meta-key and is never settable via io().with()
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS, "attacker.key");
        try {
            step.generateProgram(graph, null);
            fail("the approved-graph-config-keys meta-key must not be settable via io().with()");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS));
        }
    }

    @Test
    public void shouldRejectReaderClassNameFromUntrustedTraversal() {
        // an FQCN supplied via the IO.reader token must not bypass the approved-format guard
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(IO.reader, "org.attacker.Rce");
        try {
            step.generateProgram(graph, null);
            fail("a reader FQCN from a remote traversal must be rejected in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }

    @Test
    public void shouldAcceptReaderKeywordFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(IO.reader, IO.graphson);
        step.generateProgram(graph, null); // a built-in keyword is always allowed
        assertEquals(GraphSONInputFormat.class.getName(),
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    @Test
    public void shouldAcceptApprovedReaderClassNameFromUntrustedTraversal() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, "org.provider.CustomInputFormat");
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith(IO.reader, "org.provider.CustomInputFormat");
        step.generateProgram(graph, null); // approved via the operator's approvedClasses list
        assertEquals("org.provider.CustomInputFormat",
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    // The write path (configureForWrite) resolves the writer format symmetrically to the read path; the following
    // mirror the reader FQCN/keyword/approved cases for the writer side.

    @Test
    public void shouldRejectWriterClassNameFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = writeStepWith(IO.writer, "org.attacker.Rce");
        try {
            step.generateProgram(graph, null);
            fail("a writer FQCN from a remote traversal must be rejected in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }

    @Test
    public void shouldAcceptWriterKeywordFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = writeStepWith(IO.writer, IO.graphson);
        step.generateProgram(graph, null); // a built-in keyword is always allowed
        assertEquals(GraphSONOutputFormat.class.getName(),
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
    }

    @Test
    public void shouldAcceptApprovedWriterClassNameFromUntrustedTraversal() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, "org.provider.CustomOutputFormat");
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = writeStepWith(IO.writer, "org.provider.CustomOutputFormat");
        step.generateProgram(graph, null); // approved via the operator's approvedClasses list
        assertEquals("org.provider.CustomOutputFormat",
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
    }

    // Request isolation: io() must configure a per-request copy of the graph, never the shared, long-lived HadoopGraph
    // configuration reused across requests. The following cover the isolation seam directly.

    @Test
    public void shouldIsolateGraphConfigurationIntoADistinctInstance() {
        final HadoopGraph shared = HadoopGraph.open(new BaseConfiguration());
        final Graph local = HadoopIoStep.isolateGraphConfiguration(shared);
        assertNotSame("io() must run against a request-local graph, not the shared one", shared, local);
        assertNotSame("the request-local graph must have its own configuration instance",
                shared.configuration(), local.configuration());
        // mutating either configuration must not affect the other (isolation in both directions)
        local.configuration().setProperty("only.on.local", "v");
        assertFalse(shared.configuration().containsKey("only.on.local"));
        shared.configuration().setProperty("only.on.shared", "v");
        assertFalse(local.configuration().containsKey("only.on.shared"));
    }

    @Test
    public void shouldCarryPristineKeysIntoIsolatedConfiguration() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        config.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, "org.provider.CustomInputFormat");
        final HadoopGraph shared = HadoopGraph.open(config);
        final Graph local = HadoopIoStep.isolateGraphConfiguration(shared);
        // the copy carries every operator key forward, so trust and approved-format seeding stay intact
        assertEquals(true, local.configuration().getBoolean(OlapClassLoadingPolicy.TRUSTED));
        assertEquals("org.provider.CustomInputFormat",
                local.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    @Test
    public void shouldReturnAFreshInstanceForEachIsolationCall() {
        // each call yields a brand-new request-local graph/config (no caching or reuse); a structural guard, not
        // concurrency coverage -- see shouldIsolateConcurrentRequestsFromEachOther.
        final HadoopGraph shared = HadoopGraph.open(new BaseConfiguration());
        final Graph a = HadoopIoStep.isolateGraphConfiguration(shared);
        final Graph b = HadoopIoStep.isolateGraphConfiguration(shared);
        assertNotSame(a, b);
        assertNotSame(a.configuration(), b.configuration());
    }

    @Test
    public void shouldIsolateConcurrentRequestsFromEachOther() throws Exception {
        // Two io() requests configuring against the SAME shared graph on two threads at once must each see only their
        // own reader/input location, and must leave the shared graph unmutated. Isolation is structural (each request
        // configures its own copy), so this is deterministic under any interleaving; the barrier forces the two
        // requests to configure concurrently, exercising concurrent reads of the shared configuration.
        final HadoopGraph shared = HadoopGraph.open(new BaseConfiguration());
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Map<String, String> readerByThread = new ConcurrentHashMap<>();
        final Map<String, String> locationByThread = new ConcurrentHashMap<>();
        final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        final BiConsumer<String, String> configureRequest = (name, file) -> {
            try {
                final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), file);
                step.setMode(ReadWriting.Mode.READING);
                barrier.await(); // release both threads together so they configure concurrently
                final Graph local = step.resolveComputeGraph(shared);
                step.generateProgram(local, null);
                readerByThread.put(name, local.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
                locationByThread.put(name, local.configuration().getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
            } catch (final Throwable t) {
                errors.add(t);
            }
        };

        final Thread a = new Thread(() -> configureRequest.accept("A", "a.kryo"));
        final Thread b = new Thread(() -> configureRequest.accept("B", "b.json"));
        a.start();
        b.start();
        a.join();
        b.join();

        assertTrue("no request should error: " + errors, errors.isEmpty());
        // each request observed only its own reader + input location (Gryo/a.kryo for A, GraphSON/b.json for B)
        assertEquals(GryoInputFormatName(), readerByThread.get("A"));
        assertEquals("a.kryo", locationByThread.get("A"));
        assertEquals(GraphSONInputFormat.class.getName(), readerByThread.get("B"));
        assertEquals("b.json", locationByThread.get("B"));
        // and neither concurrent request mutated the shared, long-lived graph configuration
        assertFalse(shared.configuration().containsKey(Constants.GREMLIN_HADOOP_GRAPH_READER));
        assertFalse(shared.configuration().containsKey(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
    }

    @Test
    public void shouldNotMutateSharedConfigurationWhenConfiguringAnIsolatedCopy() {
        // the fix: generateProgram writes onto the request-local copy resolveComputeGraph hands it, leaving the shared
        // graph configuration untouched, so a later request inherits none of this request's reader/input location.
        final HadoopGraph shared = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), "graph.kryo");
        step.setMode(ReadWriting.Mode.READING);

        final Graph local = step.resolveComputeGraph(shared);
        step.generateProgram(local, null);

        // the request-local copy carries the request's settings ...
        assertEquals(GryoInputFormatName(), local.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
        assertEquals("graph.kryo", local.configuration().getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
        // ... while the shared, long-lived configuration is left unmutated
        assertFalse(shared.configuration().containsKey(Constants.GREMLIN_HADOOP_GRAPH_READER));
        assertFalse(shared.configuration().containsKey(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
    }

    @Test
    public void shouldApplyApprovedWithKeyToTheRequestLocalCopyOnly() {
        // an operator-approved with() key is applied to the request-local copy and must not leak onto the shared graph
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS, "my.graph.option");
        final HadoopGraph shared = HadoopGraph.open(config);
        final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), "graph.kryo");
        step.setMode(ReadWriting.Mode.READING);
        step.configure("my.graph.option", "v");

        final Graph local = step.resolveComputeGraph(shared);
        step.generateProgram(local, null);

        // the value landed on the request-local copy ...
        assertEquals("v", local.configuration().getString("my.graph.option"));
        // ... but the shared graph never received it
        assertFalse(shared.configuration().containsKey("my.graph.option"));
    }

    @Test
    public void shouldLeaveSharedConfigurationCleanWhenARequestFailsPartway() {
        // configureForRead writes the reader/input location to the graph before addParametersToConfiguration rejects an
        // unapproved with() key, so the request fails partway with those already written -- onto the request-local copy.
        final HadoopGraph shared = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), "graph.kryo");
        step.setMode(ReadWriting.Mode.READING);
        step.configure("unapproved.key", "v");
        final Graph local = step.resolveComputeGraph(shared);
        try {
            step.generateProgram(local, null);
            fail("an unapproved with() key must fail the request");
        } catch (final IllegalArgumentException expected) {
            // expected: the request failed after the reader/input location were already written to the local copy
        }
        assertEquals("the partial write must have landed on the request-local copy", GryoInputFormatName(),
                local.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
        // ... and none of the failed request's partial mutations reached the shared graph
        assertFalse(shared.configuration().containsKey(Constants.GREMLIN_HADOOP_GRAPH_READER));
        assertFalse(shared.configuration().containsKey(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
    }

    @Test
    public void shouldFailClosedWhenIsolatingANonHadoopGraph() {
        // a security-isolation primitive must fail closed: a non-HadoopGraph must be rejected, never returned unchanged
        // (which would silently hand back the shared, long-lived graph and reinstate the cross-request leak).
        try {
            HadoopIoStep.isolateGraphConfiguration(EmptyGraph.instance());
            fail("request isolation must reject a non-HadoopGraph rather than silently returning the shared graph");
        } catch (final IllegalStateException ise) {
            assertTrue(ise.getMessage(), ise.getMessage().contains("HadoopGraph"));
        }
    }

    private static String GryoInputFormatName() {
        return org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat.class.getName();
    }
}
