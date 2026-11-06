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
package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapClassLoadingPolicy;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapConfigKeyPolicy;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.SparkVertexProgramInterceptor;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.interceptor.SparkCloneVertexProgramInterceptor;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.interceptor.SparkStarBarrierInterceptor;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the OLAP computer-config key allow-list enforced at {@link SparkGraphComputer#configure(String, Object)}
 * (delegating to {@link AbstractHadoopGraphComputer#checkConfigurationKeyPermitted(String)}) and the interceptor
 * class validation at {@link SparkGraphComputer#resolveInterceptorClass(String)}. No Spark context is created.
 */
public class SparkGraphComputerConfigSecurityTest {

    private static SparkGraphComputer computer(final boolean trusted, final String approvedKeysCsv) {
        final Configuration c = new BaseConfiguration();
        if (trusted)
            c.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        if (null != approvedKeysCsv)
            c.setProperty(AbstractHadoopGraphComputer.APPROVED_COMPUTER_CONFIG_KEYS, approvedKeysCsv);
        return new SparkGraphComputer(HadoopGraph.open(c));
    }

    @Test
    public void shouldRejectUnknownConfigKeyWhenUntrusted() {
        try {
            computer(false, null).configure("spark.executor.extraJavaOptions", "-Devil");
            fail("an unapproved computer-config key must be rejected for an untrusted traversal");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(AbstractHadoopGraphComputer.APPROVED_COMPUTER_CONFIG_KEYS));
        }
    }

    @Test
    public void shouldAllowFrameworkKeyWhenUntrusted() {
        // TinkerPop's own Spark strategies set these via configure() during normal execution
        computer(false, null).configure(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, true);
        computer(false, null).configure(Constants.GREMLIN_SPARK_SKIP_GRAPH_CACHE, true);
        computer(false, null).configure(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR, "x");
    }

    @Test
    public void shouldAllowOperatorApprovedKeyWhenUntrusted() {
        computer(false, "my.custom.key,another.key").configure("my.custom.key", "v"); // must not throw
        computer(false, "my.custom.key,another.key").configure("another.key", "v");   // must not throw
    }

    @Test
    public void shouldRejectSelfElevationWhenUntrusted() {
        // the trust meta-key is categorically refused by configure(), independent of trust/allow-list
        try {
            computer(false, null).configure(OlapClassLoadingPolicy.TRUSTED, true);
            fail("a traversal must not grant itself trust through configure()");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldRejectMetaKeysEvenIfOperatorApprovesThem() {
        // even if an operator mistakenly lists the meta-keys, they remain non-settable via configure()
        for (final String metaKey : new String[]{OlapClassLoadingPolicy.TRUSTED, OlapClassLoadingPolicy.APPROVED_CLASSES,
                OlapConfigKeyPolicy.APPROVED_COMPUTER_CONFIG_KEYS, OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS}) {
            try {
                computer(false, metaKey).configure(metaKey, "whatever");
                fail("meta-key '" + metaKey + "' must never be settable via configure()");
            } catch (final IllegalArgumentException iae) {
                assertTrue(iae.getMessage().contains(metaKey));
            }
        }
    }

    @Test
    public void shouldNotBreakFluentSettersWhenUntrusted() throws Exception {
        // the typed fluent setters are embedded-only and must keep working on a default (untrusted) graph
        final SparkGraphComputer computer = computer(false, null)
                .serializer(GryoSerializer.class)
                .master("local[4]")
                .kryoRegistrationRequired(true)
                .persistContext(false);
        // assert the values actually landed (not merely that the setters didn't throw): a setter silently routed
        // through a no-op would still "not throw", so read back the resulting spark configuration.
        final Configuration spark = sparkConfigurationOf(computer);
        assertEquals("local[4]", spark.getString(SparkLauncher.SPARK_MASTER));
        assertEquals(GryoSerializer.class.getCanonicalName(), spark.getString(Constants.SPARK_SERIALIZER));
        assertTrue(spark.getBoolean(Constants.SPARK_KRYO_REGISTRATION_REQUIRED, false));
        assertFalse(spark.getBoolean(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true));
    }

    // Reads SparkGraphComputer's private spark configuration for value assertions. Test-local reflection is preferable
    // to widening production API with a getter that would expose internal mutable configuration.
    private static Configuration sparkConfigurationOf(final SparkGraphComputer computer) throws Exception {
        final Field field = SparkGraphComputer.class.getDeclaredField("sparkConfiguration");
        field.setAccessible(true);
        return (Configuration) field.get(computer);
    }

    @Test
    public void shouldLandAllowedConfigKeyValueWhenUntrusted() throws Exception {
        // an allowed key must not only pass the gate but actually store its value on the spark configuration
        final SparkGraphComputer framework = computer(false, null);
        framework.configure(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, true);
        assertTrue(sparkConfigurationOf(framework).getBoolean(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, false));

        final SparkGraphComputer approved = computer(false, "my.custom.key");
        approved.configure("my.custom.key", "v");
        assertEquals("v", sparkConfigurationOf(approved).getString("my.custom.key"));
    }

    @Test
    public void shouldAllowAnyConfigKeyWhenTrusted() {
        // trusted deployment retains full pass-through
        computer(true, null).configure("spark.executor.extraJavaOptions", "-Dok");
        computer(true, null).configure("fs.file.impl", "some.FileSystem");
    }

    // the gate must also fire through the real OLAP surface: Computer.apply() loops configure(). This throws before
    // any SparkContext is created, so it needs no cluster.
    @Test
    public void shouldBlockInjectedConfigKeyThroughComputerApplyWhenUntrusted() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration()); // untrusted
        try {
            Computer.compute(SparkGraphComputer.class)
                    .configure("spark.executor.extraJavaOptions", "-Devil")
                    .apply(graph);
            fail("Computer.apply must reject an injected computer-config key on an untrusted graph");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(AbstractHadoopGraphComputer.APPROVED_COMPUTER_CONFIG_KEYS));
        }
    }

    // The interceptor KEY is a permitted framework key, so its VALUE (a class name) must be validated at the load site:
    // untrusted deployments may resolve only approved interceptors, so an injected value cannot drive arbitrary class
    // loading, while the built-ins that normal execution relies on stay resolvable.
    @Test
    public void shouldRestrictInterceptorClassAtLoadSiteWhenUntrusted() throws Exception {
        assertEquals(SparkCloneVertexProgramInterceptor.class,
                computer(false, null).resolveInterceptorClass(SparkCloneVertexProgramInterceptor.class.getName()));
        assertEquals(SparkStarBarrierInterceptor.class,
                computer(false, null).resolveInterceptorClass(SparkStarBarrierInterceptor.class.getName()));
        try {
            computer(false, null).resolveInterceptorClass("java.lang.Runtime");
            fail("an unapproved interceptor class must be rejected before loading on an untrusted graph");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }

    // Trusted deployments load any interceptor directly, but a non-interceptor class still fails fast with a clear
    // assignability message (rather than a later ClassCastException deep in the job).
    @Test
    public void shouldLoadAnyAssignableInterceptorButRejectNonInterceptorWhenTrusted() throws Exception {
        assertEquals(SparkStarBarrierInterceptor.class,
                computer(true, null).resolveInterceptorClass(SparkStarBarrierInterceptor.class.getName()));
        try {
            computer(true, null).resolveInterceptorClass("java.lang.Runtime");
            fail("a class that is not a SparkVertexProgramInterceptor must be rejected even when trusted");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(SparkVertexProgramInterceptor.class.getName()));
        }
    }

    // An interceptor the operator declared in the graph configuration (gremlin.hadoop.vertexProgramInterceptor) is
    // auto-approved for untrusted resolution -- mirroring how HadoopIoStep auto-seeds its reader/writer -- so a
    // provider's configured interceptor resolves without also being listed in gremlin.io.approvedClasses.
    @Test
    public void shouldApproveInterceptorDeclaredInGraphConfigurationWhenUntrusted() throws Exception {
        final Configuration c = new BaseConfiguration(); // untrusted
        c.setProperty(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR, ConfiguredInterceptor.class.getName());
        final SparkGraphComputer computer = new SparkGraphComputer(HadoopGraph.open(c));
        assertEquals(ConfiguredInterceptor.class,
                computer.resolveInterceptorClass(ConfiguredInterceptor.class.getName()));
    }

    // The auto-seed must approve ONLY the operator-declared interceptor, not an arbitrary other class: the seed reads
    // the pristine graph config, never the post-merge value being resolved. Declaring one interceptor must not
    // approve a different injected class -- a regression that seeded from the resolved (post-merge) value would be
    // circular and this test would catch it.
    @Test
    public void shouldNotApproveArbitraryInterceptorWhenAnotherIsDeclaredWhenUntrusted() throws Exception {
        final Configuration c = new BaseConfiguration(); // untrusted
        c.setProperty(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR, ConfiguredInterceptor.class.getName());
        final SparkGraphComputer computer = new SparkGraphComputer(HadoopGraph.open(c));
        try {
            computer.resolveInterceptorClass("java.lang.Runtime");
            fail("declaring one interceptor must not approve an arbitrary other class");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage(), iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }

    // separate lists: a key approved on the io() (graph) list must NOT open it for computer.configure(), which reads
    // only the computer list. Proves the two surfaces keep independent operator allow-lists.
    @Test
    public void shouldNotHonorGraphConfigKeysListForComputerConfigureWhenUntrusted() {
        final Configuration c = new BaseConfiguration();
        c.setProperty(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS, "spark.executor.extraJavaOptions");
        final SparkGraphComputer computer = new SparkGraphComputer(HadoopGraph.open(c));
        try {
            computer.configure("spark.executor.extraJavaOptions", "-Devil");
            fail("configure() must not honor the io() (graph) approved-key list");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapConfigKeyPolicy.APPROVED_COMPUTER_CONFIG_KEYS));
        }
    }

    // a class rejected by resolveInterceptorClass's trusted branch must not run its static initializer (it is loaded
    // non-initializing before the assignability check) -- proven with a probe referenced only by name.
    @Test
    public void shouldNotInitializeRejectedInterceptorClassWhenTrusted() throws Exception {
        assertFalse("probe must start uninitialized", NON_INTERCEPTOR_PROBE_INITIALIZED);
        try {
            computer(true, null).resolveInterceptorClass(NonInterceptorProbe.class.getName());
            fail("a non-interceptor class must be rejected even when trusted");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(SparkVertexProgramInterceptor.class.getName()));
        }
        assertFalse("a rejected interceptor class must not have run its static initializer (non-init load)",
                NON_INTERCEPTOR_PROBE_INITIALIZED);
    }

    /** Set by {@link NonInterceptorProbe}'s static initializer; kept on the (already-loaded) test class so reading it
     * does not initialize the probe. */
    public static volatile boolean NON_INTERCEPTOR_PROBE_INITIALIZED = false;

    /** A non-interceptor class initialized only if something resolves it and runs its initializer. */
    public static final class NonInterceptorProbe {
        static {
            NON_INTERCEPTOR_PROBE_INITIALIZED = true;
        }
    }

    // A non-built-in interceptor used only to exercise operator-declared interceptor resolution; apply() is never
    // invoked by resolveInterceptorClass (which only resolves the class), so it is a no-op.
    public static final class ConfiguredInterceptor implements SparkVertexProgramInterceptor<VertexProgram> {
        @Override
        public JavaPairRDD<Object, VertexWritable> apply(final VertexProgram vertexProgram,
                                                         final JavaPairRDD<Object, VertexWritable> graph,
                                                         final SparkMemory memory) {
            return graph;
        }
    }
}
