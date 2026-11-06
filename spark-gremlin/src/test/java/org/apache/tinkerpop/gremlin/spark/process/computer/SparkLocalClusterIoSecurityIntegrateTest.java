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
import org.apache.spark.launcher.SparkLauncher;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapClassLoadingPolicy;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.clone.CloneVertexProgram;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Validates the OLAP trust boundary on <b>real, forked executor JVMs</b> via Spark {@code local-cluster} mode. Unlike
 * {@link SparkHadoopIoSecurityIntegrateTest} (local[N], in-JVM), the gated code here runs in a separate executor
 * process, so cross-JVM behavior is exercised for real. Two boundaries are covered:
 * <ul>
 *   <li><b>script reader/writer gates</b> — a {@code ScriptInputFormat} job (blocked untrusted / allowed trusted) at
 *       the executor's record reader and, symmetrically, a {@code ScriptOutputFormat} job at the executor's record
 *       writer, proving {@code gremlin.io.trusted} is serialized across the JVM boundary into the executor task
 *       configuration on both the read and write sides.</li>
 *   <li><b>computer-config gate</b> — an untrusted traversal that injects {@code spark.executor.extraJavaOptions}
 *       through {@code withComputer(Computer.compute(...).configure(...))} is refused at the driver {@code configure()}
 *       boundary before any executor launches; the trusted counterpart proves that same option genuinely crosses into
 *       a forked executor JVM (a probe vertex program observes the injected {@code -D}), so the guarded sink is a real
 *       executor-JVM code-injection vector. The interceptor class name is likewise validated at its load site.</li>
 * </ul>
 * <p/>
 * Requires a real Spark distribution: set {@code SPARK_HOME} (local-cluster uses it to launch executors). When that
 * distribution is a <i>binary</i> release, also export {@code SPARK_SCALA_VERSION} matching the distro (e.g.
 * {@code 2.12} for {@code spark-*-bin-hadoop3}); Spark's {@code AbstractCommandBuilder#getScalaVersion()} otherwise
 * probes for a source-tree layout ({@code $SPARK_HOME/launcher/target/scala-*}) that a binary release lacks and the
 * executor fork dies with "Cannot find any build directories." That variable is read via {@code System.getenv} at JVM
 * start, so it must be exported into the (fork of the) test JVM — it cannot be supplied through Spark configuration.
 * The test skips when {@code SPARK_HOME} is absent, so it is inert in ordinary CI.
 * <p/>
 * Run: {@code SPARK_HOME=<dist> SPARK_SCALA_VERSION=2.12 mvn integration-test failsafe:verify -pl spark-gremlin
 * -DskipTests -DskipIntegrationTests=false -Dit.test=SparkLocalClusterIoSecurityIntegrateTest}.
 */
public class SparkLocalClusterIoSecurityIntegrateTest {

    private static final String SCRIPT_FILE = "gremlin.hadoop.scriptInputFormat.script";
    private static final String SCRIPT_OUTPUT_FILE = "gremlin.hadoop.scriptOutputFormat.script";

    @After
    @Before
    public void cleanup() {
        Spark.close();
        HadoopPools.close();
        KryoShimServiceLoader.close();
    }

    private static Configuration localClusterConfiguration(final boolean trusted) {
        assumeTrue("SPARK_HOME must point to a real Spark distribution to launch local-cluster executors",
                System.getenv("SPARK_HOME") != null && !System.getenv("SPARK_HOME").isEmpty());
        final Configuration c = new BaseConfiguration();
        // two forked executor JVMs, 1 core / 1g each -> genuine cross-JVM config serialization
        c.setProperty(SparkLauncher.SPARK_MASTER, "local-cluster[2,1,1024]");
        c.setProperty(Constants.SPARK_SERIALIZER, GryoSerializer.class.getCanonicalName());
        c.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        c.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        // ship the driver's classpath to the forked executors so our patched jars load there
        final String cp = System.getProperty("java.class.path");
        c.setProperty("spark.executor.extraClassPath", cp);
        c.setProperty("spark.driver.extraClassPath", cp);
        c.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, ScriptInputFormat.class.getCanonicalName());
        c.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, TestFiles.PATHS.get("grateful-dead.txt"));
        c.setProperty(SCRIPT_FILE, TestFiles.PATHS.get("script-input-grateful-dead.groovy"));
        if (trusted)
            c.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        return c;
    }

    /**
     * Switches the given config to a built-in Gryo read of the modern graph (no {@code ScriptInputFormat}), so a
     * test's only possible failure is the behavior under test rather than the (separately covered) script-reader gate.
     */
    private static Configuration withBuiltInGryoInput(final Configuration c) {
        c.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        c.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, TestFiles.PATHS.get("tinkerpop-modern-v3.kryo"));
        c.clearProperty(SCRIPT_FILE);
        return c;
    }

    /**
     * Reads the grateful-dead graph via built-in Gryo (no {@code ScriptInputFormat}) and writes it via
     * {@code ScriptOutputFormat}, so the only script-gated component is the write side. The output script requires
     * grateful-dead labels ({@code song}/{@code artist}), so this reads that graph rather than the modern one.
     */
    private static Configuration withScriptOutputOfGratefulDead(final Configuration c) {
        c.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        c.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, TestFiles.PATHS.get("grateful-dead-v3.kryo"));
        c.clearProperty(SCRIPT_FILE);
        c.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, ScriptOutputFormat.class.getCanonicalName());
        c.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION,
                TestHelper.makeTestDataDirectory(SparkLocalClusterIoSecurityIntegrateTest.class, UUID.randomUUID().toString()));
        c.setProperty(SCRIPT_OUTPUT_FILE, TestFiles.PATHS.get("script-output-grateful-dead.groovy"));
        return c;
    }

    private static boolean stackMentions(final Throwable t, final String marker) {
        final java.io.StringWriter sw = new java.io.StringWriter();
        t.printStackTrace(new java.io.PrintWriter(sw));
        return sw.toString().contains(marker);
    }

    /** Recursively reports whether {@code dir} holds any non-empty regular file (an OutputFormat wrote part files). */
    private static boolean containsNonEmptyFile(final File dir) {
        final File[] children = dir.listFiles();
        if (null == children)
            return dir.isFile() && dir.length() > 0;
        for (final File child : children) {
            if (child.isDirectory()) {
                if (containsNonEmptyFile(child)) return true;
            } else if (child.length() > 0) {
                return true;
            }
        }
        return false;
    }

    // Baseline sanity on a real cluster: a built-in Gryo read runs across forked executors.
    @Test
    public void shouldReadBuiltInGryoOnForkedExecutors() {
        final Configuration c = withBuiltInGryoInput(localClusterConfiguration(true));
        final long count = GraphFactory.open(c).traversal().withComputer(SparkGraphComputer.class).V().count().next();
        assertTrue("built-in Gryo OLAP must run on forked executors; got " + count, count > 0);
    }

    // Cross-JVM block: ScriptInputFormat, untrusted -> executor task fails with our trust-flag message.
    @Test
    public void shouldBlockScriptInputFormatOnForkedExecutorsWhenUntrusted() {
        final Configuration c = localClusterConfiguration(false);
        try {
            GraphFactory.open(c).traversal().withComputer(SparkGraphComputer.class).V().count().next();
            fail("ScriptInputFormat must be blocked on the executor when gremlin.io.trusted is not set");
        } catch (final Throwable t) {
            assertTrue("executor-side rejection must name the trust flag; stack was: " + t,
                    stackMentions(t, OlapClassLoadingPolicy.TRUSTED));
        }
    }

    // Cross-JVM allow (the validity gate): ScriptInputFormat, trusted -> flag reaches the executor, job runs.
    @Test
    public void shouldAllowScriptInputFormatOnForkedExecutorsWhenTrusted() {
        final Configuration c = localClusterConfiguration(true);
        final long count = GraphFactory.open(c).traversal().withComputer(SparkGraphComputer.class).V().count().next();
        assertTrue("a trusted ScriptInputFormat job must run on forked executors; got " + count, count > 0);
    }

    // Cross-JVM allow via the narrower opt-in: untrusted, but the operator approved ScriptInputFormat in
    // gremlin.io.approvedClasses. Proves the approved-class list reaches the executor's ScriptRecordReader gate and
    // enables the job cross-JVM, not just the global trust flag.
    @Test
    public void shouldAllowScriptInputFormatOnForkedExecutorsWhenFormatApproved() {
        final Configuration c = localClusterConfiguration(false);
        c.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, ScriptInputFormat.class.getName());
        final long count = GraphFactory.open(c).traversal().withComputer(SparkGraphComputer.class).V().count().next();
        assertTrue("an approved ScriptInputFormat job must run on forked executors (untrusted + approvedClasses); got " + count, count > 0);
    }

    // Cross-JVM block, WRITE side: ScriptOutputFormat, untrusted -> the executor's ScriptRecordWriter refuses to
    // construct, so the write task fails with our trust-flag message. The read side is built-in Gryo, so the only
    // script-gated component is the writer.
    @Test
    public void shouldBlockScriptOutputFormatOnForkedExecutorsWhenUntrusted() {
        final Configuration c = withScriptOutputOfGratefulDead(localClusterConfiguration(false));
        try {
            GraphFactory.open(c).compute(SparkGraphComputer.class)
                    .program(CloneVertexProgram.build().create()).submit().get();
            fail("ScriptOutputFormat must be blocked on the executor when gremlin.io.trusted is not set");
        } catch (final Throwable t) {
            assertTrue("executor-side write rejection must name the trust flag; stack was: " + t,
                    stackMentions(t, OlapClassLoadingPolicy.TRUSTED));
        }
    }

    // Cross-JVM allow, WRITE side: ScriptOutputFormat, trusted -> the writer runs on the executor (compiling and
    // executing the output script per vertex) and produces output, proving the flag reaches the write task too.
    @Test
    public void shouldAllowScriptOutputFormatOnForkedExecutorsWhenTrusted() throws Exception {
        final Configuration c = withScriptOutputOfGratefulDead(localClusterConfiguration(true));
        final File outputLocation = new File(c.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));
        GraphFactory.open(c).compute(SparkGraphComputer.class)
                .program(CloneVertexProgram.build().create()).submit().get();
        assertTrue("a trusted ScriptOutputFormat job must write script output on forked executors",
                containsNonEmptyFile(outputLocation));
    }

    // Cross-JVM allow, WRITE side via the narrower opt-in: untrusted, but the operator approved ScriptOutputFormat in
    // gremlin.io.approvedClasses -> the executor's ScriptRecordWriter gate passes and produces output. Symmetric to the
    // read-side approved-format scenario.
    @Test
    public void shouldAllowScriptOutputFormatOnForkedExecutorsWhenFormatApproved() throws Exception {
        final Configuration c = withScriptOutputOfGratefulDead(localClusterConfiguration(false));
        c.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, ScriptOutputFormat.class.getName());
        final File outputLocation = new File(c.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));
        GraphFactory.open(c).compute(SparkGraphComputer.class)
                .program(CloneVertexProgram.build().create()).submit().get();
        assertTrue("an approved ScriptOutputFormat job must write output on forked executors (untrusted + approvedClasses)",
                containsNonEmptyFile(outputLocation));
    }

    // an untrusted OLAP traversal that injects spark.executor.extraJavaOptions through the real
    // withComputer(Computer.compute(...).configure(...)) surface -> refused at the driver configure() boundary, so the
    // executor-JVM option never reaches any forked process (no context is even created). Uses built-in Gryo input so
    // the only thing that can reject the traversal is the config-key gate (not the ScriptInputFormat reader).
    @Test
    public void shouldBlockInjectedExecutorJvmOptionOnUntrustedGraph() {
        final Configuration c = withBuiltInGryoInput(localClusterConfiguration(false));
        try {
            GraphFactory.open(c).traversal()
                    .withComputer(Computer.compute(SparkGraphComputer.class)
                            .configure(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-Dtinkerpop.tierb.rce=owned"))
                    .V().count().next();
            fail("an untrusted OLAP traversal must not inject " + SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS
                    + " via configure()");
        } catch (final Throwable t) {
            assertTrue("refusal must name the operator approval key; stack was: " + t,
                    stackMentions(t, AbstractHadoopGraphComputer.APPROVED_COMPUTER_CONFIG_KEYS));
        }
    }

    // the same injected spark.executor.extraJavaOptions is permitted on a trusted graph and provably crosses into a
    // forked executor JVM: a probe vertex program runs on the executor and reports (via Memory) that it observed the
    // injected -D. This proves the guarded sink is real and executor-affecting, not theoretical.
    @Test
    public void shouldReachForkedExecutorJvmWithInjectedOptionWhenTrusted() throws Exception {
        final Configuration c = withBuiltInGryoInput(localClusterConfiguration(true));
        final String property = "tinkerpop.tierb.canary";
        final String value = "REACHED";
        final ComputerResult result = GraphFactory.open(c)
                .compute(SparkGraphComputer.class)
                .configure(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-D" + property + "=" + value)
                .program(SystemPropertyProbeVertexProgram.forProperty(property, value))
                .submit().get();
        assertTrue("the injected -D" + property + " must be observed inside a forked executor JVM",
                result.memory().<Boolean>get(SystemPropertyProbeVertexProgram.MEMORY_OBSERVED));
    }

    // probe integrity (paired with the test above): the same probe, run WITHOUT injecting the option, must not observe
    // the canary on any executor -- proving the probe genuinely detects the injected -D (so the "reach" test above is
    // meaningful) and that an ordinary trusted job carries no such property.
    @Test
    public void shouldNotObserveCanaryOnForkedExecutorWithoutInjection() throws Exception {
        final Configuration c = withBuiltInGryoInput(localClusterConfiguration(true));
        final ComputerResult result = GraphFactory.open(c)
                .compute(SparkGraphComputer.class)
                .program(SystemPropertyProbeVertexProgram.forProperty("tinkerpop.tierb.canary", "REACHED"))
                .submit().get();
        assertFalse("no executor JVM should see the canary when the option is not injected",
                result.memory().<Boolean>get(SystemPropertyProbeVertexProgram.MEMORY_OBSERVED));
    }

    // the interceptor KEY is a framework-approved config key, so an untrusted OLAP job may set it via configure() --
    // but its VALUE (a class name) is validated at the load site by resolveInterceptorClass during submit(): an
    // unapproved interceptor class is refused (message names gremlin.io.approvedClasses), so it cannot drive arbitrary
    // reflective class loading. A CloneVertexProgram job deterministically reaches the interceptor branch -- when the
    // interceptor key is already present, the computer skips its built-in auto-selection and resolves the set value.
    @Test
    public void shouldBlockInjectedUnapprovedInterceptorClassWhenUntrusted() {
        final Configuration c = withBuiltInGryoInput(localClusterConfiguration(false));
        try {
            GraphFactory.open(c).compute(SparkGraphComputer.class)
                    .configure(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR, "java.lang.Runtime")
                    .program(CloneVertexProgram.build().create())
                    .submit().get();
            fail("an untrusted OLAP job must not resolve an unapproved vertexProgramInterceptor class");
        } catch (final Throwable t) {
            assertTrue("interceptor rejection must name the approved-classes key; stack was: " + t,
                    stackMentions(t, OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }
}
