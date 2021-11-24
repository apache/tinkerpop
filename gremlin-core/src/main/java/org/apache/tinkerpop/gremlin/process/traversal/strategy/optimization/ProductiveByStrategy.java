/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Grouping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Takes an argument of {@code by()} and wraps it {@link CoalesceStep} so that the result is either the initial
 * {@link Traversal} argument or {@code null}. In this way, the {@code by()} is always "productive". This strategy
 * is an "optimization" but it is perhaps more of a "decoration", but it should follow
 * {@link ByModulatorOptimizationStrategy} which features optimizations relevant to this one.
 */
public class ProductiveByStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {
    public static final String PRODUCTIVE_KEYS = "productiveKeys";

    private static final ProductiveByStrategy INSTANCE = new ProductiveByStrategy(Collections.emptyList());
    private final List<String> productiveKeys;
    private static final ConstantTraversal<?,?> nullTraversal = new ConstantTraversal<>(null);
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(ByModulatorOptimizationStrategy.class));

    private ProductiveByStrategy(final List<String> productiveKeys) {
        this.productiveKeys = productiveKeys;
    }

    public static ProductiveByStrategy create(final Configuration configuration) {
        return new ProductiveByStrategy(new ArrayList<>((Collection<String>) configuration.getProperty(PRODUCTIVE_KEYS)));
    }

    /**
     * Gets the standard configuration of this strategy that will apply it for all conditions. It is this version of
     * the strategy that is added as standard. Note that it may be helpful to configure a custom instance using the
     * {@link #build()} method in cases where there is certainty that a {@code by()} will be productive as it will
     * reduce the complexity of the traversal and perhaps improve the execution of other optimizations.
     */
    public static ProductiveByStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // ByModulating steps should also be TraversalParent in most cases - if they aren't this strategy
        // probably doesn't need to fuss with them
        TraversalHelper.getStepsOfAssignableClass(ByModulating.class, traversal).stream().
                filter(bm -> bm instanceof TraversalParent).forEach(bm -> {
            final TraversalParent parentStep = (TraversalParent) bm;
            final boolean parentStepIsGrouping = parentStep instanceof Grouping;
            parentStep.getLocalChildren().forEach(child -> {
                // Grouping requires a bit of special handling because of TINKERPOP-2627 which is a bug that has
                // some unexpected behavior for coalesce() as the value traversal. Grouping also has so inconsistencies
                // in how null vs filter behavior works and that behavior needs to stay to not break in 3.5.x
                if (!parentStepIsGrouping) {
                    if (child instanceof ValueTraversal && hasKeyNotKnownAsProductive((ValueTraversal) child)) {
                        wrapValueTraversalInCoalesce(parentStep, child);
                    } else if (!(child.getEndStep() instanceof ReducingBarrierStep)) {
                        // ending reducing barrier will always return something so seems safe to not bother wrapping
                        // that up in coalesce().
                        final Traversal.Admin extractedChildTraversal = new DefaultGraphTraversal<>();
                        TraversalHelper.removeToTraversal(child.getStartStep(), EmptyStep.instance(), extractedChildTraversal);
                        child.addStep(new CoalesceStep(child, extractedChildTraversal, nullTraversal));

                        // replace so that internally the parent step gets to re-initialize the child as it may need to.
                        try {
                            parentStep.replaceLocalChild(child, child);
                        } catch (IllegalStateException ignored) {
                            // ignore situations where the parent traversal doesn't support replacement. in those cases
                            // we simply retain whatever the original behavior was even if it is inconsistent
                        }
                    }
                } else {
                    if (child instanceof ValueTraversal && ((Grouping) parentStep).getKeyTraversal() == child &&
                            hasKeyNotKnownAsProductive((ValueTraversal) child) && !containsValidByPass((ValueTraversal) child)) {
                        wrapValueTraversalInCoalesce(parentStep, child);
                    }
                }
            });
        });
    }

    /**
     * Validate that the {@link ValueTraversal} needs to be wrapped. It can be skipped if a bypass is already in place
     * or if there isn't a {@link CoalesceStep} at the start or if there isn't a {@code null} in the
     * {@link CoalesceStep}.
     */
    private boolean containsValidByPass(final ValueTraversal vt) {
        if (null == vt.getBypassTraversal()) return false;
        if (!(vt.getStartStep() instanceof CoalesceStep)) return false;
        final CoalesceStep coalesceStep = (CoalesceStep) vt.getStartStep();
        final List<Traversal> children = coalesceStep.getLocalChildren();
        final Traversal lastChild = children.get(children.size() - 1);
        return lastChild == nullTraversal ||
                (lastChild instanceof ConstantTraversal && ((ConstantTraversal) lastChild).next() == null) ||
                (lastChild.asAdmin().getEndStep() instanceof ConstantStep && ((ConstantStep) lastChild.asAdmin().getEndStep()).getConstant() == null);
    }

    private void wrapValueTraversalInCoalesce(final TraversalParent parentStep, final Traversal.Admin<Object, Object> child) {
        final Traversal.Admin temp = new DefaultGraphTraversal<>();
        temp.addStep(new CoalesceStep(temp, child.clone(), nullTraversal));
        temp.setParent(parentStep);
        ((ValueTraversal<Object, Object>) child).setBypassTraversal(temp);
    }

    /**
     * Determines if the {@link ValueTraversal} references a productive key.
     */
    private boolean hasKeyNotKnownAsProductive(final ValueTraversal child) {
        return productiveKeys.isEmpty() ||
                child.getBypassTraversal() == null && !productiveKeys.contains(child.getPropertyKey());
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STRATEGY, ProductiveByStrategy.class.getCanonicalName());
        map.put(PRODUCTIVE_KEYS, this.productiveKeys);
        return new MapConfiguration(map);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private final ArrayList<String> productiveKeys = new ArrayList<>();

        private Builder() {}

        /**
         * Specify the list of property keys that should always be productive for {@code by(String)}. If the keys are
         * not set, then all `by(String)` are productive. Arguments to {@code by()} that are not of type
         * {@link ValueTraversal} will not be considered.
         */
        public Builder productiveKeys(final String key, final String... rest) {
            this.productiveKeys.clear();
            productiveKeys.add(key);
            productiveKeys.addAll(Arrays.asList(rest));
            return this;
        }

        /**
         * Specify the list of property keys that should always be productive for {@code by(String)}. If the keys are
         * not set, then all `by(String)` are productive.  Arguments to {@code by()} that are not of type
         * {@link ValueTraversal} will not be considered.
         */
        public Builder productiveKeys(final Collection<String> keys) {
            this.productiveKeys.clear();
            this.productiveKeys.addAll(keys);
            return this;
        }

        public ProductiveByStrategy create() {
            return new ProductiveByStrategy(this.productiveKeys);
        }
    }
}
