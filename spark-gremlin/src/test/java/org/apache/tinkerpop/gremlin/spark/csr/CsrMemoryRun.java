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
package org.apache.tinkerpop.gremlin.spark.csr;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.BuildOptions;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.BuildStats;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.HeapSnapshotBuilder;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.HybridSnapshotBuilder;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.SnapshotBuilder;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.StreamingSnapshotBuilder;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.CsrNative;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.CsrSuperStep;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrMemoryBudgetException;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.strategy.CsrNativeStrategy;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SnapshotSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.GryoSnapshotSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphSnapshotSource;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Runs one CSR memory measurement in this JVM and prints one JSON record. It is a manually invoked spike utility, not a
 * test; {@code spark-gremlin/src/test/csr-memory/csrbench.py} launches it, one JVM per measurement. Never run it
 * from a unit test: it loads whole graphs.
 * <p/>
 * <pre>
 * java [jvm flags] org.apache.tinkerpop.gremlin.spark.csr.CsrMemoryRun
 *   --mode build|open|query
 *   --system tinkergraph|csr-native|csr-facade|tinkergraph-computer|spark   (query mode; open mode: tinkergraph|csr)
 *   --dataset &lt;.kryo file&gt;               (required for tinkergraph*, spark, and build)
 *   --snapshot &lt;snapshot dir&gt;            (csr-* and open csr; build writes here and it must not exist)
 *   --query-file &lt;file&gt; --query &lt;id&gt;     (query mode; the file is described below)
 *   --warm &lt;n&gt;                           warm runs after the cold run, default 3
 *   --timeout-seconds &lt;n&gt;                soft timeout for the whole run, outcome TIMEOUT
 *   --csr-budget &lt;bytes&gt;                 csrMemoryBudget of csr-native, default 1073741824
 *   --builder heap|streaming|hybrid       (build) default streaming
 *   --source gryo|tinkergraph             (build) default gryo
 *   --builder-budget &lt;bytes&gt;             (build) default 268435456
 *   --verify-checksums true|false         (open, csr) default false
 *   --spark-master &lt;url&gt;                 default local[*]
 *   --spark-conf k=v                      repeatable Spark, Hadoop or gremlin.* property
 *   --work-dir &lt;dir&gt;                     scratch and Spark output directory, default a new temp directory
 *   --sample-interval-ms &lt;n&gt;             heap sampler period, default 1000
 *   --series-out &lt;file&gt;                  heap series CSV
 *   --out &lt;file&gt;                         result JSON, which is also printed on stdout as "CSRBENCH-RESULT {...}"
 *   --help
 * </pre>
 * The exit status is 0 for outcome COMPLETED and 2 for every other recorded outcome (OOM, BUDGET_EXCEEDED, TIMEOUT,
 * UNSUPPORTED, ERROR). 1 is a usage error. Phase markers {@code CSRBENCH-PHASE <epochMillis> <phase> <begin|end>} are
 * printed on stdout for the phases load, open, build, cold, warm-1, warm-2 and so on.
 * <p/>
 * <b>Systems.</b> {@code tinkergraph} loads the Gryo file into a TinkerGraph (list vertex property cardinality and null
 * property values allowed, like {@code CsrSnapshotBuildBenchmark}; the JVM property
 * {@code -Dcsrbench.tinkergraph.cardinality=single} switches to single cardinality, which the graph-algorithm matrix
 * uses) and runs OLTP. {@code tinkergraph-computer} is the same graph with
 * {@code g.withComputer()}. {@code csr-native} opens the snapshot and runs with {@code csrMemoryBudget} set from
 * {@code --csr-budget} and {@code csrScratchDirectory} under the work directory; {@code csr-facade} adds
 * {@code withoutStrategies(CsrNativeStrategy)}. {@code spark} runs {@code g.withComputer(SparkGraphComputer)} over a
 * {@link HadoopGraph} that reads the Gryo file with {@code GryoInputFormat} in one JVM ({@code spark.master}
 * {@code local[*]} by default, the Kryo serializer with {@code GryoRegistrator}, graph and persist storage levels
 * {@code MEMORY_AND_DISK}, {@code spark.local.dir} and the Gremlin output location under the work directory, the Spark
 * UI off). {@code --spark-conf} entries are applied last and win.
 * <p/>
 * <b>Query mode.</b> Queries come from a file of lines {@code id | small|large | systems | gremlin-lang | ordered}
 * (see {@code csr-queries.txt}). A system that the line does not list is reported UNSUPPORTED before anything is
 * loaded, as is a query that a graph computer rejects ({@code UnsupportedOperationException}, a traversal
 * verification failure or a "not supported" message). The text is evaluated with {@link GremlinLangScriptEngine} with
 * {@code g} bound to the system's traversal source, run once cold and {@code --warm} more times, each time fully
 * iterating the results. The count is the number of results; the hash is a sum of per-result hashes (order-insensitive)
 * or, for an {@code ordered} query, a sequence hash. The hash is taken over a canonical text of each result in which
 * map entries and set elements are sorted, so the same result hashes the same across systems. It comes from the cold
 * run; {@code extra.warmConsistent} says whether the warm runs agreed. {@code extra.resultSample} holds the canonical
 * text of the first few results of the cold run, so that small summaries can be compared by eye, and on
 * {@code csr-native} {@code extra.csrSuperSteps} and {@code extra.csrPlan} give the number of native super steps and
 * the compiled step list, which show whether the query fused.
 * <p/>
 * <b>Vertex-program steps.</b> {@code pageRank()}, {@code connectedComponent()} and {@code shortestPath()}, with their
 * {@code PageRank}, {@code ConnectedComponent} and {@code ShortestPath} {@code with()} options, are part of
 * gremlin-lang, so such queries are ordinary lines that list only {@code tinkergraph-computer} and {@code spark}
 * (CSR has no graph computer, and OLTP TinkerGraph rejects the steps); the other systems report UNSUPPORTED. The
 * query reads what the program wrote ({@code values('pageRank')}, {@code by('component')}, the paths) in the same
 * traversal, so count and hash cover it. On Spark the steps chain jobs through the Gryo graph writer and output
 * location configured below.
 * <p/>
 * <b>Build and open.</b> Build mode loads the TinkerGraph first when {@code --source tinkergraph} (timed separately as
 * {@code loadMillis}) and then times the build; {@code build.phases} are the {@link BuildStats} phases, and
 * {@code peakScratchBytes} is {@link BuildStats#peakDiskBytes()}, {@code build.timers} are the
 * {@link BuildStats#timers()} in milliseconds (the edge-scan split) and {@code build.peakBudgetBytes} is
 * {@link BuildStats#peakBudgetBytes()}. Open mode times {@link CsrGraph#open} (or the Gryo
 * load for tinkergraph), records the heap after a full GC in {@code extra.retainedHeapBytes}, and then runs
 * {@code g.V().count()} and {@code g.V().out().count()} cold ({@code coldMillis} is the count; the one-hop times are in
 * {@code extra}) and once warm.
 * <p/>
 * <b>Outcomes.</b> {@link OutOfMemoryError} is OOM: the program drops its graph references and releases a reserve
 * allocated at start before writing the record. {@link CsrMemoryBudgetException} is BUDGET_EXCEEDED with
 * {@code errorOwner} the owner string, for example {@code "Dedup#3 seen keys"}, and {@code ownerClass} {@code result}
 * for owners that hold the result (a {@code result map}, {@code Fold} state, the {@code AggregateSideEffect},
 * {@code GroupSideEffect} and {@code GroupCountSideEffect} state, and the in-memory group, count and group-reducer
 * tables) and {@code working} for every other owner, which is a defect when the query's result is small. A run still
 * going when the soft timeout passes is interrupted and recorded as TIMEOUT; if it does not stop within 30 seconds the
 * record is written from the timer thread and the JVM halts. Anything else is ERROR.
 * <p/>
 * The result JSON has the fields of the contract shared with the driver, plus an {@code extra} object (retained heap,
 * open-mode query times, warm consistency, the Java and Spark settings used) that the driver may ignore.
 */
public final class CsrMemoryRun {

    private static final String RESULT_PREFIX = "CSRBENCH-RESULT ";
    private static final String PHASE_PREFIX = "CSRBENCH-PHASE ";
    private static final long TIMEOUT_GRACE_MILLIS = 30_000L;
    private static final int RESERVE_BYTES = 8 * 1024 * 1024;
    private static final String CARDINALITY_PROPERTY = "csrbench.tinkergraph.cardinality";
    private static final int PLAN_CHARS = 4000;
    private static final int SAMPLE_RESULTS = 10;
    private static final int SAMPLE_CHARS = 2000;

    private static final Set<String> SYSTEMS = Set.of("tinkergraph", "csr-native", "csr-facade", "tinkergraph-computer", "spark");

    private static final String USAGE = String.join(System.lineSeparator(),
            "Usage: CsrMemoryRun --mode build|open|query [options]",
            "  --system tinkergraph|csr-native|csr-facade|tinkergraph-computer|spark   (open: tinkergraph|csr)",
            "  --dataset <.kryo>   --snapshot <dir>   --query-file <file> --query <id>   --warm <n> (3)",
            "  --timeout-seconds <n>   --csr-budget <bytes> (1073741824)   --builder heap|streaming|hybrid (streaming)",
            "  --source gryo|tinkergraph (gryo)   --builder-budget <bytes> (268435456)",
            "  --verify-checksums true|false (false)   --spark-master <url> (local[*])   --spark-conf k=v (repeatable)",
            "  --work-dir <dir>   --sample-interval-ms <n> (1000)   --series-out <file>   --out <file>   --help",
            "See the class Javadoc for details.");

    // held back and freed on OutOfMemoryError so that the result can still be written
    private static volatile byte[] reserve;

    private final Config config;
    private final long startMillis = System.currentTimeMillis();
    private final long startNanos = System.nanoTime();
    private final HeapSampler sampler;

    // run state, cleared by release() on OutOfMemoryError
    private TinkerGraph tinkerGraph;
    private CsrGraph csrGraph;
    private HadoopGraph hadoopGraph;
    private GraphTraversalSource g;
    private Traversal<?, ?> current;

    private String outcome;
    private String error;
    private String errorOwner;
    private String ownerClass;
    private volatile boolean timedOut;
    private volatile boolean finished;
    private boolean written;
    private Thread mainThread;

    private Long loadMillis, openMillis, buildMillis, coldMillis;
    private final List<Long> warmMillis = new ArrayList<>();
    private Long resultCount;
    private String resultHash;
    private String resultSize;
    private Long csrPeakBytes, csrScratchBytes;
    private Map<String, Object> buildInfo;
    private final Map<String, Object> extra = new LinkedHashMap<>();

    private CsrMemoryRun(final Config config) {
        this.config = config;
        this.sampler = new HeapSampler(config.sampleIntervalMs, config.seriesOut);
    }

    public static void main(final String[] args) {
        final Config config;
        try {
            config = Config.parse(args);
        } catch (UsageException e) {
            if (e.getMessage() != null) System.err.println(e.getMessage());
            System.err.println(USAGE);
            System.exit(e.help ? 0 : 1);
            return;
        }
        reserve = new byte[RESERVE_BYTES];
        System.exit(new CsrMemoryRun(config).execute());
    }

    // ---------------------------------------------------------------- run

    private int execute() {
        mainThread = Thread.currentThread();
        sampler.start();
        startWatchdog();
        try {
            switch (config.mode) {
                case "build":
                    runBuild();
                    break;
                case "open":
                    runOpen();
                    break;
                default:
                    runQuery();
                    break;
            }
            if (outcome == null) outcome = "COMPLETED";
        } catch (Throwable t) {
            classify(t);
        }
        finished = true;
        try {
            if (config.system != null && config.system.equals("spark")) Spark.close();
        } catch (Throwable ignored) {
            // the record matters more than a clean Spark shutdown
        }
        writeResult();
        return "COMPLETED".equals(outcome) ? 0 : 2;
    }

    private void startWatchdog() {
        if (config.timeoutSeconds <= 0) return;
        final Thread watchdog = new Thread(() -> {
            try {
                Thread.sleep(config.timeoutSeconds * 1000L);
                if (finished) return;
                timedOut = true;
                mainThread.interrupt();
                final long grace = System.currentTimeMillis() + TIMEOUT_GRACE_MILLIS;
                while (!finished && System.currentTimeMillis() < grace) Thread.sleep(200);
                if (!finished) {
                    // the run ignored the interrupt, so record it from here
                    outcome = "TIMEOUT";
                    error = "no response to the interrupt " + (TIMEOUT_GRACE_MILLIS / 1000) + " seconds after the "
                            + config.timeoutSeconds + " second timeout";
                    writeResult();
                    Runtime.getRuntime().halt(2);
                }
            } catch (InterruptedException ignored) {
                // main finished
            }
        }, "csrbench-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
    }

    private void checkTimeout() {
        if (timedOut) throw new TimeoutSignal();
    }

    // ---------------------------------------------------------------- query mode

    private void runQuery() throws Exception {
        require(config.system, "--system");
        require(config.queryFile, "--query-file");
        require(config.queryId, "--query");
        final QueryDef query = QueryDef.find(config.queryFile, config.queryId);
        resultSize = query.size;
        if (!query.allows(config.system)) {
            outcome = "UNSUPPORTED";
            error = "query " + query.id + " does not list system " + config.system;
            return;
        }

        loadSystem();
        extra.put("queryText", query.text);
        final GremlinLangScriptEngine engine = new GremlinLangScriptEngine();

        phase("cold", true);
        final long coldStart = System.nanoTime();
        try {
            final Digest digest = runOnce(engine, query);
            resultCount = digest.count;
            resultHash = digest.hex();
            coldMillis = millisSince(coldStart);
            extra.put("resultSample", digest.sample());
        } catch (Throwable t) {
            coldMillis = millisSince(coldStart);
            throw t;
        } finally {
            phase("cold", false);
        }

        boolean consistent = true;
        for (int i = 1; i <= config.warm; i++) {
            final String name = "warm-" + i;
            phase(name, true);
            final long start = System.nanoTime();
            try {
                final Digest digest = runOnce(engine, query);
                warmMillis.add(millisSince(start));
                consistent &= digest.count == resultCount && digest.hex().equals(resultHash);
            } finally {
                phase(name, false);
            }
        }
        extra.put("warmConsistent", consistent);
    }

    private Digest runOnce(final GremlinLangScriptEngine engine, final QueryDef query) throws Exception {
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);
        Traversal<?, ?> traversal = null;
        try {
            final Object evaluated = engine.eval(query.text, bindings);
            if (!(evaluated instanceof Traversal)) {
                throw new IllegalStateException("query " + query.id + " did not evaluate to a traversal");
            }
            traversal = (Traversal<?, ?>) evaluated;
            current = traversal;
            final Digest digest = new Digest(query.ordered);
            while (traversal.hasNext()) {
                digest.add(traversal.next());
                if (timedOut) throw new TimeoutSignal();
            }
            return digest;
        } finally {
            recordCsr(traversal);
            if (traversal != null) {
                try {
                    traversal.close();
                } catch (Exception ignored) {
                    // not worth losing the result
                }
            }
            current = null;
        }
    }

    private void recordCsr(final Traversal<?, ?> traversal) {
        if (traversal == null || !"csr-native".equals(config.system)) return;
        if (!extra.containsKey("csrPlan")) {
            // the compiled plan, once: which steps the native strategy replaced (CsrSuperStep) and which stayed
            try {
                extra.put("csrSuperSteps", TraversalHelper.getStepsOfAssignableClassRecursively(
                        CsrSuperStep.class, traversal.asAdmin()).size());
                final String plan = String.valueOf(traversal.asAdmin().getSteps());
                extra.put("csrPlan", plan.length() > PLAN_CHARS ? plan.substring(0, PLAN_CHARS) + "..." : plan);
            } catch (RuntimeException ignored) {
                // the traversal may not have been compiled
            }
        }
        try {
            final long peak = CsrNative.lastPeakBytes(traversal);
            final long scratch = CsrNative.lastScratchBytes(traversal);
            if (peak >= 0) csrPeakBytes = csrPeakBytes == null ? peak : Math.max(csrPeakBytes, peak);
            if (scratch >= 0) csrScratchBytes = csrScratchBytes == null ? scratch : Math.max(csrScratchBytes, scratch);
        } catch (RuntimeException ignored) {
            // the traversal may not have been compiled
        }
    }

    private void loadSystem() throws Exception {
        switch (config.system) {
            case "tinkergraph":
                loadTinkerGraph();
                g = tinkerGraph.traversal();
                break;
            case "tinkergraph-computer":
                loadTinkerGraph();
                g = tinkerGraph.traversal().withComputer();
                break;
            case "csr-native":
                openCsr(false);
                g = csrGraph.traversal()
                        .with(CsrNativeStrategy.OPTION_MEMORY_BUDGET, config.csrBudget)
                        .with(CsrNativeStrategy.OPTION_SCRATCH_DIRECTORY, scratchDir("csr-scratch").toString());
                break;
            case "csr-facade":
                openCsr(false);
                g = csrGraph.traversal().withoutStrategies(CsrNativeStrategy.class);
                break;
            case "spark":
                require(config.dataset, "--dataset");
                hadoopGraph = HadoopGraph.open(sparkConfiguration());
                g = hadoopGraph.traversal().withComputer(SparkGraphComputer.class);
                break;
            default:
                throw new UsageException("unknown system " + config.system, false);
        }
    }

    /**
     * The Hadoop graph configuration for Spark in this JVM. Spark reads the Gryo file inside every job, so there is no
     * load phase.
     */
    private Configuration sparkConfiguration() throws IOException {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        conf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getName());
        conf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getName());
        conf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, Paths.get(config.dataset).toAbsolutePath().toString());
        conf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, scratchDir("spark-output").resolve("out").toString());
        conf.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        conf.setProperty(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER, SparkGraphComputer.class.getName());
        conf.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        conf.setProperty(Constants.GREMLIN_SPARK_GRAPH_STORAGE_LEVEL, "MEMORY_AND_DISK");
        conf.setProperty(Constants.GREMLIN_SPARK_PERSIST_STORAGE_LEVEL, "MEMORY_AND_DISK");
        conf.setProperty("spark.master", config.sparkMaster);
        conf.setProperty("spark.app.name", "CsrMemoryRun");
        conf.setProperty(Constants.SPARK_SERIALIZER, KryoSerializer.class.getName());
        conf.setProperty(Constants.SPARK_KRYO_REGISTRATOR, GryoRegistrator.class.getName());
        conf.setProperty(Constants.SPARK_KRYO_REGISTRATION_REQUIRED, false);
        conf.setProperty("spark.local.dir", scratchDir("spark-local").toString());
        conf.setProperty("spark.ui.enabled", false);
        for (final String[] kv : config.sparkConf) conf.setProperty(kv[0], kv[1]);
        final Map<String, Object> used = new LinkedHashMap<>();
        final java.util.Iterator<String> keys = conf.getKeys();
        while (keys.hasNext()) {
            final String key = keys.next();
            used.put(key, String.valueOf(conf.getProperty(key)));
        }
        extra.put("sparkConf", used);
        return conf;
    }

    private void loadTinkerGraph() {
        require(config.dataset, "--dataset");
        phase("load", true);
        final long start = System.nanoTime();
        try {
            // list cardinality and null values, so that multi-properties and nulls in the file (the rich generator mode writes
            // both) survive, as in CsrSnapshotBuildBenchmark. -Dcsrbench.tinkergraph.cardinality=single overrides the
            // cardinality for single-valued data: TraversalVertexProgram writes its compute keys with the default
            // cardinality, so on a list graph a traverser that stays at a vertex (a barrier() inside repeat()) fails
            final String cardinality = System.getProperty(CARDINALITY_PROPERTY, "list");
            extra.put("tinkergraphCardinality", cardinality);
            final Configuration conf = new BaseConfiguration();
            conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, cardinality);
            conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, true);
            tinkerGraph = TinkerGraph.open(conf);
            try (GraphTraversalSource loader = tinkerGraph.traversal()) {
                loader.io(config.dataset).read().iterate();
            } catch (Exception e) {
                throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
            }
        } finally {
            loadMillis = millisSince(start);
            phase("load", false);
        }
    }

    private void openCsr(final boolean verify) {
        require(config.snapshot, "--snapshot");
        phase("open", true);
        final long start = System.nanoTime();
        try {
            final Configuration conf = new BaseConfiguration();
            conf.setProperty(CsrGraph.GREMLIN_CSR_DIRECTORY, config.snapshot);
            conf.setProperty(CsrGraph.GREMLIN_CSR_VERIFY_CHECKSUMS, verify);
            csrGraph = CsrGraph.open(conf);
        } finally {
            openMillis = millisSince(start);
            phase("open", false);
        }
    }

    // ---------------------------------------------------------------- open mode

    private void runOpen() throws Exception {
        require(config.system, "--system");
        final boolean csr = config.system.startsWith("csr");
        if (csr) {
            openCsr(config.verifyChecksums);
            g = csrGraph.traversal();
            extra.put("verifyChecksums", config.verifyChecksums);
        } else {
            loadTinkerGraph();
            g = tinkerGraph.traversal();
        }

        System.gc();
        final Runtime runtime = Runtime.getRuntime();
        extra.put("retainedHeapBytes", runtime.totalMemory() - runtime.freeMemory());

        phase("cold", true);
        final long countCold, hopCold;
        try {
            long t = System.nanoTime();
            extra.put("vertices", g.V().count().next());
            countCold = millisSince(t);
            t = System.nanoTime();
            extra.put("oneHopResults", g.V().out().count().next());
            hopCold = millisSince(t);
        } finally {
            phase("cold", false);
        }
        coldMillis = countCold;
        extra.put("oneHopColdMillis", hopCold);

        phase("warm-1", true);
        try {
            long t = System.nanoTime();
            g.V().count().next();
            warmMillis.add(millisSince(t));
            t = System.nanoTime();
            g.V().out().count().next();
            extra.put("oneHopWarmMillis", millisSince(t));
        } finally {
            phase("warm-1", false);
        }
    }

    // ---------------------------------------------------------------- build mode

    private void runBuild() throws Exception {
        require(config.dataset, "--dataset");
        require(config.snapshot, "--snapshot");
        final boolean fromGryo = "gryo".equals(config.source);
        if (!fromGryo) loadTinkerGraph();

        final SnapshotBuilder builder = "heap".equals(config.builder) ? new HeapSnapshotBuilder()
                : "hybrid".equals(config.builder) ? new HybridSnapshotBuilder() : new StreamingSnapshotBuilder();
        final BuildOptions options = BuildOptions.builder()
                .memoryBudgetBytes(config.builderBudget)
                .scratchDirectory(scratchDir("build-scratch"))
                .build();

        phase("build", true);
        final long start = System.nanoTime();
        BuildStats stats = null;
        try (SnapshotSource source = fromGryo ? new GryoSnapshotSource(Paths.get(config.dataset)) : new TinkerGraphSnapshotSource(tinkerGraph)) {
            stats = builder.build(source, Paths.get(config.snapshot), options);
        } finally {
            buildMillis = millisSince(start);
            phase("build", false);
        }

        final List<Object> phases = new ArrayList<>();
        for (final BuildStats.Phase p : stats.phases()) {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", p.name());
            m.put("millis", p.elapsedNanos() / 1_000_000L);
            m.put("diskBytes", p.diskBytes());
            phases.add(m);
        }
        buildInfo = new LinkedHashMap<>();
        buildInfo.put("phases", phases);
        buildInfo.put("publishedBytes", stats.totalSegmentBytes());
        buildInfo.put("peakScratchBytes", stats.peakDiskBytes());
        final Map<String, Object> timers = new LinkedHashMap<>();
        for (final Map.Entry<String, Long> t : stats.timers().entrySet()) timers.put(t.getKey(), t.getValue() / 1_000_000L);
        buildInfo.put("timers", timers);
        buildInfo.put("peakBudgetBytes", stats.peakBudgetBytes());
        extra.put("builder", config.builder);
        extra.put("source", config.source);
        extra.put("builderBudget", config.builderBudget);
    }

    // ---------------------------------------------------------------- outcome

    private void classify(final Throwable thrown) {
        // an OutOfMemoryError needs the reserve and the references released first, before anything allocates
        for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof OutOfMemoryError) {
                release();
                outcome = "OOM";
                error = describe(t);
                return;
            }
        }
        // the owner of a budget exception is read before the traversal is dropped
        for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof CsrMemoryBudgetException) {
                final CsrMemoryBudgetException budget = (CsrMemoryBudgetException) t;
                recordCsr(current);
                outcome = "BUDGET_EXCEEDED";
                error = budget.getMessage();
                errorOwner = budget.owner();
                ownerClass = ownerClass(budget.owner());
                return;
            }
        }
        if (timedOut || thrown instanceof TimeoutSignal) {
            outcome = "TIMEOUT";
            error = "interrupted after the " + config.timeoutSeconds + " second timeout";
            return;
        }
        final String text = describeChain(thrown);
        if (text.contains("java.lang.OutOfMemoryError")) {
            release();
            outcome = "OOM";
            error = text;
            return;
        }
        if (config.system != null && config.system.endsWith("computer") || "spark".equals(config.system)) {
            if (computerRejected(thrown)) {
                outcome = "UNSUPPORTED";
                error = text;
                return;
            }
        }
        outcome = "ERROR";
        error = text;
        if (System.getenv("CSRBENCH_STACKTRACES") != null) thrown.printStackTrace();
    }

    private static boolean computerRejected(final Throwable thrown) {
        for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof UnsupportedOperationException) return true;
            final String name = t.getClass().getName();
            if (name.endsWith("VerificationException")) return true;
            final String message = t.getMessage();
            if (message != null) {
                final String lower = message.toLowerCase(Locale.ROOT);
                if (lower.contains("not supported") || lower.contains("not support ") || lower.contains("unsupported")
                        || lower.contains("not compatible with the graph computer")) return true;
            }
        }
        return false;
    }

    /**
     * Whether an owner string holds the result rather than working state. Owners read
     * {@code "<operator>#<id> <state>"} (see {@code AbstractCsrOperator.owner}). The result is held by a
     * {@code result map} (spilling group and groupCount), by Fold state, by the side-effect writers
     * (AggregateSideEffect, GroupSideEffect, GroupCountSideEffect) and by the in-memory group tables and group reducer
     * state of the group terminals, whose size is the number of groups.
     */
    static String ownerClass(final String owner) {
        if (owner == null) return null;
        final boolean spillWorking = owner.contains("partition") || owner.contains("spill");
        if (owner.contains("result map")) return "result";
        if (owner.startsWith("Fold#")) return "result";
        if (owner.startsWith("AggregateSideEffect") || owner.startsWith("GroupSideEffect")
                || owner.startsWith("GroupCountSideEffect")) return spillWorking ? "working" : "result";
        if (!spillWorking && (owner.endsWith(" count table") || owner.endsWith(" group table")
                || owner.endsWith(" groups") || owner.endsWith(" buffer"))) return "result";
        return "working";
    }

    /**
     * Drops what the run holds so that the record can be written after an OutOfMemoryError.
     */
    private void release() {
        current = null;
        g = null;
        tinkerGraph = null;
        csrGraph = null;
        hadoopGraph = null;
        reserve = null;
    }

    private static String describe(final Throwable t) {
        return t.getClass().getName() + ": " + t.getMessage();
    }

    private static String describeChain(final Throwable thrown) {
        final StringBuilder sb = new StringBuilder();
        int depth = 0;
        for (Throwable t = thrown; t != null && depth < 6; t = t.getCause() == t ? null : t.getCause(), depth++) {
            if (depth > 0) sb.append(" <- ");
            sb.append(describe(t));
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------- result

    /**
     * The process's I/O counters from {@code /proc/self/io}, read when the result is written so that the last writes
     * are counted. {@code read_bytes} and {@code write_bytes} are storage-level (pages read from disk, pages dirtied,
     * including through memory maps); {@code rchar} and {@code wchar} are bytes passed to read and write system calls.
     * Null where the file does not exist.
     */
    private static Map<String, Object> readProcIo() {
        try {
            final Map<String, Object> io = new LinkedHashMap<>();
            for (final String line : Files.readAllLines(Paths.get("/proc/self/io"), StandardCharsets.US_ASCII)) {
                final int colon = line.indexOf(':');
                if (colon > 0) io.put(line.substring(0, colon).trim(), Long.parseLong(line.substring(colon + 1).trim()));
            }
            return io;
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private synchronized void writeResult() {
        if (written) return;
        written = true;
        try {
            sampler.stop();
        } catch (Throwable ignored) {
            // report what was sampled
        }
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", config.mode);
        result.put("system", config.system);
        result.put("dataset", config.dataset);
        result.put("query", config.queryId);
        result.put("resultSize", resultSize);
        result.put("outcome", outcome);
        result.put("error", error);
        result.put("errorOwner", errorOwner);
        result.put("ownerClass", ownerClass);

        final Map<String, Object> timings = new LinkedHashMap<>();
        timings.put("loadMillis", loadMillis);
        timings.put("openMillis", openMillis);
        timings.put("buildMillis", buildMillis);
        timings.put("coldMillis", coldMillis);
        timings.put("warmMillis", new ArrayList<Object>(warmMillis));
        timings.put("warmMedianMillis", median(warmMillis));
        result.put("timings", timings);

        final Map<String, Object> res = new LinkedHashMap<>();
        res.put("count", resultCount);
        res.put("hash", resultHash);
        result.put("result", res);

        result.put("heap", sampler.summary());

        if (config.system != null && config.system.startsWith("csr")) {
            final Map<String, Object> csr = new LinkedHashMap<>();
            csr.put("budget", "csr-native".equals(config.system) && "query".equals(config.mode) ? (Object) config.csrBudget : null);
            csr.put("peakBytes", csrPeakBytes);
            csr.put("scratchBytes", csrScratchBytes);
            result.put("csr", csr);
        } else {
            result.put("csr", null);
        }
        result.put("build", buildInfo);
        result.put("io", readProcIo());

        final Map<String, Object> jvm = new LinkedHashMap<>();
        jvm.put("version", System.getProperty("java.version"));
        jvm.put("args", new ArrayList<Object>(ManagementFactory.getRuntimeMXBean().getInputArguments()));
        result.put("jvm", jvm);

        extra.put("startMillis", startMillis);
        extra.put("totalMillis", (System.nanoTime() - startNanos) / 1_000_000L);
        result.put("extra", extra);

        final String json = Json.write(result);
        try {
            if (config.out != null) Files.write(Paths.get(config.out), json.getBytes(StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException e) {
            System.err.println("could not write " + config.out + ": " + e);
        }
        System.out.println(RESULT_PREFIX + json);
        System.out.flush();
    }

    private static Long median(final List<Long> values) {
        if (values.isEmpty()) return null;
        final List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        final int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
    }

    // ---------------------------------------------------------------- helpers

    private void phase(final String name, final boolean begin) {
        if (begin) sampler.setPhase(name);
        System.out.println(PHASE_PREFIX + System.currentTimeMillis() + " " + name + (begin ? " begin" : " end"));
        System.out.flush();
        if (!begin) sampler.setPhase("idle");
    }

    private Path scratchDir(final String name) {
        try {
            if (config.workDir == null) config.workDir = Files.createTempDirectory("csr-memory-run-").toString();
            return Files.createDirectories(Paths.get(config.workDir).toAbsolutePath().resolve(name));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static long millisSince(final long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private static void require(final String value, final String option) {
        if (value == null) throw new UsageException("missing " + option, false);
    }

    // ---------------------------------------------------------------- result digest

    /**
     * Counts results and hashes them. The hash is of a canonical text so that equal results from different systems
     * hash equally even when their maps and sets iterate in different orders.
     */
    private static final class Digest {
        private final boolean ordered;
        private long count;
        private long accumulator;

        Digest(final boolean ordered) {
            this.ordered = ordered;
        }

        private final List<String> sample = new ArrayList<>();
        private int sampleChars;

        void add(final Object result) {
            final String text = canonical(result);
            final long h = hash(text);
            count++;
            if (ordered) accumulator = mix(accumulator * 31 + h);
            else accumulator += mix(h);
            if (sample.size() < SAMPLE_RESULTS && sampleChars < SAMPLE_CHARS) {
                final String kept = text.length() > SAMPLE_CHARS - sampleChars
                        ? text.substring(0, SAMPLE_CHARS - sampleChars) + "..." : text;
                sample.add(kept);
                sampleChars += kept.length();
            }
        }

        /**
         * The canonical text of the first results, at most {@link #SAMPLE_RESULTS} of them and about
         * {@link #SAMPLE_CHARS} characters, so that small results (algorithm summaries) can be read from the record.
         */
        List<Object> sample() {
            return new ArrayList<>(sample);
        }

        String hex() {
            return Long.toHexString(accumulator);
        }

        private static long hash(final String s) {
            long h = 0xcbf29ce484222325L;
            for (int i = 0; i < s.length(); i++) {
                h ^= s.charAt(i);
                h *= 0x100000001b3L;
            }
            return h;
        }

        private static long mix(long v) {
            v = (v ^ (v >>> 30)) * 0xbf58476d1ce4e5b9L;
            v = (v ^ (v >>> 27)) * 0x94d049bb133111ebL;
            return v ^ (v >>> 31);
        }

        static String canonical(final Object o) {
            if (o instanceof Map) {
                final List<String> entries = new ArrayList<>();
                for (final Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                    entries.add(canonical(e.getKey()) + "=" + canonical(e.getValue()));
                }
                Collections.sort(entries);
                return "{" + String.join(",", entries) + "}";
            }
            if (o instanceof Tree) {
                // a Tree is not a Map and iterates in hash order
                final Tree<?> tree = (Tree<?>) o;
                final List<String> entries = new ArrayList<>();
                for (final Object key : tree.rootNodes()) {
                    entries.add(canonical(key) + "=" + canonical(((Tree<Object>) tree).childAt(key)));
                }
                Collections.sort(entries);
                return "{" + String.join(",", entries) + "}";
            }
            if (o instanceof Set) {
                final List<String> items = new ArrayList<>();
                for (final Object item : (Set<?>) o) items.add(canonical(item));
                Collections.sort(items);
                return "{" + String.join(",", items) + "}";
            }
            if (o instanceof Collection) {
                final StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                for (final Object item : (Collection<?>) o) {
                    if (!first) sb.append(',');
                    first = false;
                    sb.append(canonical(item));
                }
                return sb.append(']').toString();
            }
            if (o instanceof Map.Entry) {
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                return canonical(e.getKey()) + "=" + canonical(e.getValue());
            }
            if (o != null && o.getClass().isArray()) {
                // arrays (a byte[] property value) would otherwise print as their identity
                final StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < java.lang.reflect.Array.getLength(o); i++) {
                    if (i > 0) sb.append(',');
                    sb.append(canonical(java.lang.reflect.Array.get(o, i)));
                }
                return sb.append(']').toString();
            }
            return String.valueOf(o);
        }
    }

    // ---------------------------------------------------------------- heap sampler

    /**
     * Samples the heap on a daemon thread and keeps the statistics, with the series written to a CSV file. The
     * averages are time-weighted. After-GC usage is the sum over the heap pools of the usage after their last
     * collection.
     */
    private static final class HeapSampler implements Runnable {
        private final long intervalMs;
        private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        private final List<MemoryPoolMXBean> pools = new ArrayList<>();
        private final List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();
        private BufferedWriter series;
        private volatile String phase = "init";
        private volatile boolean running = true;
        private Thread thread;
        private boolean stopped;

        private long[] used = new long[1024];
        private int samples;
        private long peakUsed, peakAfterGc, previousTime, previousUsed, previousAfterGc, totalWeight;
        private double weightedUsed, weightedAfterGc;
        private long gcCount, gcMillis, heapMax;

        HeapSampler(final long intervalMs, final String seriesFile) {
            this.intervalMs = Math.max(1, intervalMs);
            for (final MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                if (pool.getType() == MemoryType.HEAP) pools.add(pool);
            }
            if (seriesFile != null) {
                try {
                    series = Files.newBufferedWriter(Paths.get(seriesFile), StandardCharsets.UTF_8);
                    series.write("epochMillis,phase,heapUsed,heapCommitted,heapMax,afterGcUsed,nonHeapUsed,gcCount,gcMillis");
                    series.newLine();
                    series.flush();
                } catch (IOException e) {
                    System.err.println("could not open " + seriesFile + ": " + e);
                    series = null;
                }
            }
        }

        void setPhase(final String name) {
            phase = name;
        }

        void start() {
            sample();
            thread = new Thread(this, "csrbench-heap-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    sample();
                } catch (Throwable t) {
                    // an OutOfMemoryError here is reported by the main thread
                }
            }
        }

        synchronized void sample() {
            final long now = System.currentTimeMillis();
            final MemoryUsage heap = memory.getHeapMemoryUsage();
            final long heapUsed = heap.getUsed();
            long afterGc = 0;
            for (final MemoryPoolMXBean pool : pools) {
                final MemoryUsage usage = pool.getCollectionUsage();
                if (usage != null) afterGc += usage.getUsed();
            }
            long count = 0, time = 0;
            for (final GarbageCollectorMXBean c : collectors) {
                count += Math.max(0, c.getCollectionCount());
                time += Math.max(0, c.getCollectionTime());
            }
            gcCount = count;
            gcMillis = time;
            heapMax = heap.getMax();

            if (samples > 0) {
                final long dt = Math.max(0, now - previousTime);
                weightedUsed += (double) heapUsed * dt;
                weightedAfterGc += (double) afterGc * dt;
                totalWeight += dt;
            }
            if (samples == used.length) used = Arrays.copyOf(used, samples * 2);
            used[samples++] = heapUsed;
            peakUsed = Math.max(peakUsed, heapUsed);
            peakAfterGc = Math.max(peakAfterGc, afterGc);
            previousTime = now;
            previousUsed = heapUsed;
            previousAfterGc = afterGc;

            if (series != null) {
                try {
                    series.write(now + "," + phase + "," + heapUsed + "," + heap.getCommitted() + "," + heap.getMax() + ","
                            + afterGc + "," + memory.getNonHeapMemoryUsage().getUsed() + "," + count + "," + time);
                    series.newLine();
                    series.flush();
                } catch (IOException e) {
                    series = null;
                }
            }
        }

        synchronized void stopSampling() {
            if (stopped) return;
            stopped = true;
            running = false;
        }

        void stop() {
            stopSampling();
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            sample();
            synchronized (this) {
                if (series != null) {
                    try {
                        series.close();
                    } catch (IOException ignored) {
                        // nothing to do
                    }
                    series = null;
                }
            }
        }

        synchronized Map<String, Object> summary() {
            final long[] sorted = Arrays.copyOf(used, samples);
            Arrays.sort(sorted);
            final long p95 = samples == 0 ? 0 : sorted[Math.min(samples - 1, (int) Math.ceil(samples * 0.95) - 1)];
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("peakUsed", peakUsed);
            m.put("avgUsed", totalWeight > 0 ? (long) (weightedUsed / totalWeight) : previousUsed);
            m.put("p95Used", p95);
            m.put("peakAfterGc", peakAfterGc);
            m.put("avgAfterGc", totalWeight > 0 ? (long) (weightedAfterGc / totalWeight) : previousAfterGc);
            m.put("samples", samples);
            m.put("gcCount", gcCount);
            m.put("gcMillis", gcMillis);
            m.put("xmx", heapMax);
            return m;
        }
    }

    // ---------------------------------------------------------------- JSON

    /**
     * A small JSON writer for maps, lists, strings, numbers, booleans and null.
     */
    private static final class Json {
        static String write(final Object value) {
            final StringBuilder sb = new StringBuilder(2048);
            write(sb, value);
            return sb.toString();
        }

        private static void write(final StringBuilder sb, final Object v) {
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Map) {
                sb.append('{');
                boolean first = true;
                for (final Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    string(sb, String.valueOf(e.getKey()));
                    sb.append(':');
                    write(sb, e.getValue());
                }
                sb.append('}');
            } else if (v instanceof List) {
                sb.append('[');
                boolean first = true;
                for (final Object item : (List<?>) v) {
                    if (!first) sb.append(',');
                    first = false;
                    write(sb, item);
                }
                sb.append(']');
            } else if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else {
                string(sb, v.toString());
            }
        }

        private static void string(final StringBuilder sb, final String s) {
            sb.append('"');
            for (int i = 0; i < s.length(); i++) {
                final char c = s.charAt(i);
                switch (c) {
                    case '"':
                        sb.append("\\\"");
                        break;
                    case '\\':
                        sb.append("\\\\");
                        break;
                    case '\n':
                        sb.append("\\n");
                        break;
                    case '\r':
                        sb.append("\\r");
                        break;
                    case '\t':
                        sb.append("\\t");
                        break;
                    default:
                        if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                        else sb.append(c);
                }
            }
            sb.append('"');
        }
    }

    // ---------------------------------------------------------------- queries

    private static final class QueryDef {
        final String id;
        final String size;
        final List<String> systems;
        final String text;
        final boolean ordered;

        private QueryDef(final String id, final String size, final List<String> systems, final String text, final boolean ordered) {
            this.id = id;
            this.size = size;
            this.systems = systems;
            this.text = text;
            this.ordered = ordered;
        }

        boolean allows(final String system) {
            return systems.contains("*") || systems.contains(system);
        }

        static QueryDef find(final String file, final String id) throws IOException {
            int lineNumber = 0;
            for (final String raw : Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8)) {
                lineNumber++;
                final String line = raw.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                final String[] fields = line.split("\\|", 5);
                if (fields.length < 4) throw new IllegalArgumentException(file + ":" + lineNumber + ": expected id | size | systems | text");
                if (!fields[0].trim().equals(id)) continue;
                final List<String> systems = new ArrayList<>();
                for (final String s : fields[2].split(",")) {
                    final String system = s.trim();
                    if (!system.equals("*") && !SYSTEMS.contains(system)) {
                        throw new IllegalArgumentException(file + ":" + lineNumber + ": unknown system " + system);
                    }
                    systems.add(system);
                }
                final boolean ordered = fields.length == 5 && fields[4].trim().equals("ordered");
                return new QueryDef(id, fields[1].trim(), systems, fields[3].trim(), ordered);
            }
            throw new IllegalArgumentException("no query " + id + " in " + file);
        }
    }

    // ---------------------------------------------------------------- arguments

    private static final class TimeoutSignal extends RuntimeException {
        TimeoutSignal() {
            super("timeout", null, false, false);
        }
    }

    private static final class UsageException extends RuntimeException {
        final boolean help;

        UsageException(final String message, final boolean help) {
            super(message);
            this.help = help;
        }
    }

    private static final class Config {
        String mode, system, dataset, snapshot, queryFile, queryId, builder = "streaming", source = "gryo";
        String sparkMaster = "local[*]", workDir, seriesOut, out;
        int warm = 3;
        long timeoutSeconds = 0, csrBudget = 1073741824L, builderBudget = 268435456L, sampleIntervalMs = 1000L;
        boolean verifyChecksums;
        final List<String[]> sparkConf = new ArrayList<>();

        static Config parse(final String[] args) {
            final Config c = new Config();
            for (int i = 0; i < args.length; i++) {
                final String a = args[i];
                if (a.equals("--help") || a.equals("-h")) throw new UsageException(null, true);
                if (!a.startsWith("--")) throw new UsageException("unexpected argument " + a, false);
                if (i + 1 >= args.length) throw new UsageException("missing value for " + a, false);
                final String v = args[++i];
                switch (a) {
                    case "--mode": c.mode = v; break;
                    case "--system": c.system = v; break;
                    case "--dataset": c.dataset = v; break;
                    case "--snapshot": c.snapshot = v; break;
                    case "--query-file": c.queryFile = v; break;
                    case "--query": c.queryId = v; break;
                    case "--warm": c.warm = Integer.parseInt(v); break;
                    case "--timeout-seconds": c.timeoutSeconds = Long.parseLong(v); break;
                    case "--csr-budget": c.csrBudget = Long.parseLong(v); break;
                    case "--builder": c.builder = v; break;
                    case "--source": c.source = v; break;
                    case "--builder-budget": c.builderBudget = Long.parseLong(v); break;
                    case "--verify-checksums": c.verifyChecksums = Boolean.parseBoolean(v); break;
                    case "--spark-master": c.sparkMaster = v; break;
                    case "--spark-conf": {
                        final int eq = v.indexOf('=');
                        if (eq <= 0) throw new UsageException("--spark-conf expects k=v but got " + v, false);
                        c.sparkConf.add(new String[]{v.substring(0, eq), v.substring(eq + 1)});
                        break;
                    }
                    case "--work-dir": c.workDir = v; break;
                    case "--sample-interval-ms": c.sampleIntervalMs = Long.parseLong(v); break;
                    case "--series-out": c.seriesOut = v; break;
                    case "--out": c.out = v; break;
                    default: throw new UsageException("unknown option " + a, false);
                }
            }
            if (c.mode == null) throw new UsageException("missing --mode", false);
            if (!Set.of("build", "open", "query").contains(c.mode)) throw new UsageException("unknown mode " + c.mode, false);
            if (c.system != null && !SYSTEMS.contains(c.system) && !c.system.equals("csr")) {
                throw new UsageException("unknown system " + c.system, false);
            }
            if (!Set.of("heap", "streaming", "hybrid").contains(c.builder)) throw new UsageException("unknown builder " + c.builder, false);
            if (!Set.of("gryo", "tinkergraph").contains(c.source)) throw new UsageException("unknown source " + c.source, false);
            return c;
        }
    }
}
