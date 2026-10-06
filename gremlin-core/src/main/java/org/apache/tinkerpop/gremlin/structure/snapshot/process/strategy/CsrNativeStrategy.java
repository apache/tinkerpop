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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.strategy;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.provider.ProviderGValueReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Replaces provably supported regions of a traversal over a {@link CsrGraph} with native
 * {@link org.apache.tinkerpop.gremlin.structure.snapshot.process.CsrSuperStep}s (section 3 of the spike document). It
 * is a {@code ProviderOptimizationStrategy}, so it sees the final shape of the standard plan: after every decoration
 * and optimization strategy, including {@code GValueReductionStrategy}, and before finalization and verification.
 * {@code applyPrior} names {@code ProviderGValueReductionStrategy} for providers that swap it in.
 * <p/>
 * The baseline for comparison is {@code g.withoutStrategies(CsrNativeStrategy.class)}. The strategy can also be turned
 * off per traversal with {@code g.with("csrNative", false)} or for a graph with {@code gremlin.csr.native.enabled}
 * set to false; the option wins over the configuration.
 * <p/>
 * <b>Options</b> (via {@code g.with(key, value)}, each overriding the graph configuration key in parentheses):
 * <ul>
 * <li>{@code csrNative} (boolean; {@code gremlin.csr.native.enabled}) turns native execution on or off;</li>
 * <li>{@code csrMemoryBudget} (bytes; {@code gremlin.csr.native.memoryBudget}) sets the memory budget, for example
 * {@code g.with('csrMemoryBudget', 64L*1024*1024)}; the earlier key {@code csrNative.memoryBudget} is still read;</li>
 * <li>{@code csrScratchDirectory} (path; {@code gremlin.csr.native.scratchDirectory}) sets the spill directory.</li>
 * </ul>
 * The configuration keys are read from the configuration passed to {@code CsrGraph.open(Configuration)}. After the
 * traversal runs, {@link org.apache.tinkerpop.gremlin.structure.snapshot.process.CsrNative#lastPeakBytes} reports the
 * peak reserved bytes.
 * <p/>
 * It plans from the root only and is a no-op on child traversals, on graph computers and on other graphs. The work is
 * split in four: {@link Analysis} (the whole-traversal vetoes of section 3.2), {@link StepCompiler} (the compile rules),
 * {@link Planner} (region detection and the all-or-nothing rewrite) and the pre-verification here. A region is only
 * rewritten when every operator in it has a real implementation in {@link CsrOperatorFactory#shared()}, and anything
 * that is not provably equivalent stays on facades. The reasons are logged at debug level.
 */
public final class CsrNativeStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    /**
     * The graph configuration key that enables native execution; defaults to true.
     */
    public static final String ENABLED_KEY = "gremlin.csr.native.enabled";

    /**
     * The graph configuration key of the memory budget in bytes; defaults to
     * {@link CsrSettings#DEFAULT_MEMORY_BUDGET}.
     */
    public static final String MEMORY_BUDGET_KEY = "gremlin.csr.native.memoryBudget";

    /**
     * The graph configuration key of the directory for spill files; defaults to the system temporary directory.
     */
    public static final String SCRATCH_DIRECTORY_KEY = "gremlin.csr.native.scratchDirectory";

    /**
     * The graph configuration key of the batch size; defaults to {@link Batch#DEFAULT_CAPACITY}.
     */
    public static final String BATCH_SIZE_KEY = "gremlin.csr.native.batchSize";

    /**
     * The {@code OptionsStrategy} key that turns native execution on or off for one traversal.
     */
    public static final String OPTION_ENABLED = "csrNative";

    /**
     * The {@code OptionsStrategy} key that overrides the memory budget, in bytes, for one traversal.
     */
    public static final String OPTION_MEMORY_BUDGET = "csrMemoryBudget";

    /**
     * The earlier spelling of {@link #OPTION_MEMORY_BUDGET}, still accepted.
     */
    public static final String OPTION_MEMORY_BUDGET_LEGACY = "csrNative.memoryBudget";

    /**
     * The {@code OptionsStrategy} key that overrides the scratch directory for spill files for one traversal.
     */
    public static final String OPTION_SCRATCH_DIRECTORY = "csrScratchDirectory";

    private static final Logger logger = LoggerFactory.getLogger(CsrNativeStrategy.class);

    private static final CsrNativeStrategy INSTANCE = new CsrNativeStrategy();

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS =
            Collections.singleton(ProviderGValueReductionStrategy.class);

    private CsrNativeStrategy() {
    }

    public static CsrNativeStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot() || TraversalHelper.onGraphComputer(traversal)) return;
        final Graph graph = traversal.getGraph().orElse(null);
        if (!(graph instanceof CsrGraph)) return;
        final CsrGraph csr = (CsrGraph) graph;
        if (!isEnabled(traversal, csr)) return;

        final Planner planner;
        final List<Planner.Edit> edits;
        try {
            final Analysis analysis = Analysis.of(traversal);
            if (analysis.vetoReason() != null) {
                debug("stays on facades: " + analysis.vetoReason());
                return;
            }
            planner = new Planner(traversal, csr, analysis, settings(traversal, csr), CsrOperatorFactory.shared());
            edits = planner.plan();
            if (!edits.isEmpty() && planner.leavesOtherVUnfused(edits)) {
                for (final String note : planner.notes()) debug(note);
                debug("stays on facades: an otherV() could not be fused and needs the path");
                return;
            }
        } catch (final RuntimeException e) {
            // fail safe: the traversal is exactly as the standard strategies left it, because nothing was changed yet
            debug("stays on facades after a planning error: " + e);
            return;
        }
        if (logger.isDebugEnabled()) {
            for (final String note : planner.notes()) logger.debug("CsrNativeStrategy: {}", note);
        }
        if (edits.isEmpty()) return;

        // the class-inspecting verification strategies run on the unfused steps, so fusion cannot hide a violation; a
        // VerificationException propagates as it would from the later verification
        for (final TraversalStrategy<?> strategy : traversal.getStrategies().toList()) {
            if (strategy instanceof TraversalStrategy.VerificationStrategy
                    && !(strategy instanceof StandardVerificationStrategy)
                    && !(strategy instanceof ComputerVerificationStrategy)) {
                TraversalHelper.applyTraversalRecursively(strategy::apply, traversal);
            }
        }
        planner.apply(edits);
        debug("replaced " + edits.size() + " region(s)");
    }

    private static void debug(final String message) {
        if (logger.isDebugEnabled()) logger.debug("CsrNativeStrategy: {}", message);
    }

    /**
     * Whether native execution is on for the traversal: the {@code csrNative} option if the traversal has it, otherwise
     * {@code gremlin.csr.native.enabled}, which defaults to true.
     */
    public static boolean isEnabled(final Traversal.Admin<?, ?> traversal, final CsrGraph graph) {
        final Object option = options(traversal).get(OPTION_ENABLED);
        if (option != null) return option instanceof Boolean ? (Boolean) option : Boolean.parseBoolean(option.toString());
        return graph.configuration().getBoolean(ENABLED_KEY, true);
    }

    /**
     * The execution settings for the traversal: the graph configuration, with the memory budget overridden by the
     * {@code csrMemoryBudget} option and the scratch directory by the {@code csrScratchDirectory} option.
     */
    public static CsrSettings settings(final Traversal.Admin<?, ?> traversal, final CsrGraph graph) {
        final Configuration conf = graph.configuration();
        long budget = conf.getLong(MEMORY_BUDGET_KEY, CsrSettings.DEFAULT_MEMORY_BUDGET);
        final Map<String, Object> options = options(traversal);
        Object option = options.get(OPTION_MEMORY_BUDGET);
        if (option == null) option = options.get(OPTION_MEMORY_BUDGET_LEGACY);
        if (option != null) budget = option instanceof Number ? ((Number) option).longValue() : Long.parseLong(option.toString());
        final Object scratchOption = options.get(OPTION_SCRATCH_DIRECTORY);
        final String scratch = scratchOption != null ? scratchOption.toString() : conf.getString(SCRATCH_DIRECTORY_KEY, null);
        final Path scratchDirectory = scratch == null ? null : Paths.get(scratch);
        return new CsrSettings(budget, scratchDirectory, conf.getInt(BATCH_SIZE_KEY, Batch.DEFAULT_CAPACITY));
    }

    private static Map<String, Object> options(final Traversal.Admin<?, ?> traversal) {
        return traversal.getStrategies().getStrategy(OptionsStrategy.class).map(OptionsStrategy::getOptions)
                .orElse(Collections.emptyMap());
    }
}
