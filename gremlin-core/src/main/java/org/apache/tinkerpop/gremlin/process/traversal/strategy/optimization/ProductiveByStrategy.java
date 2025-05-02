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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Takes an argument of {@code by()} and wraps it {@link CoalesceStep} so that the result is either the initial
 * {@link Traversal} argument or {@code null}. In this way, the {@code by()} is always "productive". This strategy
 * is an "optimization" but it is perhaps more of a "decoration", but it should follow
 * {@link ByModulatorOptimizationStrategy} which features optimizations relevant to this one.
 */
public class ProductiveByStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {
    public static final String PRODUCTIVE_KEYS = "productiveKeys";

    private static final ProductiveByStrategy INSTANCE = new ProductiveByStrategy(Collections.emptySet());
    private final Set<String> productiveKeys;
    private static final ConstantTraversal<?,?> nullTraversal = new ConstantTraversal<>(null);
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(ByModulatorOptimizationStrategy.class));

    private ProductiveByStrategy(final Set<String> productiveKeys) {
        this.productiveKeys = productiveKeys;
    }

    public static ProductiveByStrategy create(final Configuration configuration) {
        return new ProductiveByStrategy(new HashSet<>((Collection<String>) configuration.getProperty(PRODUCTIVE_KEYS)));
    }

    public Set<String> getProductiveKeys() {
        return productiveKeys;
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
            for (Traversal.Admin child : parentStep.getLocalChildren()) {
                // just outright skip the PropertyMapStep.propertyTraversal - it is only set by SubgraphStrategy and
                // for this case it doesn't make sense to force that traversal to be productive. if it is forced then
                // PropertyMap actually gets a null Property object if the traversal is not productive and that
                // causes an NPE. It doesn't make sense to catch the NPE there really as the code wants an empty
                // Iterator<Property> rather than a productive null. Actually, the propertyTraversal isn't even a
                // by() provided Traversal, so it really doesn't make sense for this strategy to touch it. Another
                // sort of example where strategies shouldn't have to know so much about one another. also, another
                // scenario where ProductiveByStrategy isn't so good. the default behavior without this strategy
                // works so much more nicely
                if (parentStep instanceof PropertyMapStep) {
                    final Traversal.Admin propertyTraversal = ((PropertyMapStep) parentStep).getPropertyTraversal();
                    if (propertyTraversal != null && propertyTraversal.equals(child))
                        continue;
                }

                if (child instanceof ValueTraversal &&  !containsValidByPass((ValueTraversal) child) &&
                        hasKeyNotKnownAsProductive((ValueTraversal) child)) {
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
            }
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
        final Configuration conf = super.getConfiguration();
        conf.setProperty(PRODUCTIVE_KEYS, this.productiveKeys);
        return conf;
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private final HashSet<String> productiveKeys = new HashSet<>();

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
