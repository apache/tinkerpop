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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.HasNextStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;

import java.util.List;

/**
 * A step which offers a choice of two or more Traversals to take.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends BranchStep<S, E, M> {

    /**
     * Defines the manner in which the {@code ChooseStep} is configured to work.
     */
    public enum ChooseSemantics {
        /**
         * Choose is using if-then-else semantics.
         */
        IF_THEN,

        /**
         * Choose is using switch semantics.
         */
        SWITCH
    }

    private final ChooseSemantics chooseSemantics;

    /**
     * Determines if the default identity traversal is being used for the {@link Pick#unproductive} match.
     */
    private boolean hasDefaultUnproductive = true;

    /**
     * Determines if the default identity traversal is being used for the {@link Pick#none} match.
     */
    private boolean hasDefaultNone = true;

    private ChooseStep(final Traversal.Admin traversal, final Traversal.Admin<S, M> choiceTraversal,
                      final ChooseSemantics chooseSemantics) {
        super(traversal);
        this.chooseSemantics = chooseSemantics;
        this.setBranchTraversal(choiceTraversal);
        
        // defaults Pick.unproductive/none options are just identity()
        final Traversal.Admin<S,E> unproductivePassthrough = new DefaultGraphTraversal<>();
        unproductivePassthrough.addStep(new IdentityStep<>(traversal));
        final Traversal.Admin<S,E> nonePassthrough = new DefaultGraphTraversal<>();
        nonePassthrough.addStep(new IdentityStep<>(traversal));

        // calling super here to prevent hitting the Pick branch of code
        super.addChildOption((M) Pick.unproductive, unproductivePassthrough);
        super.addChildOption((M) Pick.none, nonePassthrough);
    }

    public ChooseStep(final Traversal.Admin traversal, final Traversal.Admin<S, M> choiceTraversal) {
        this(traversal, choiceTraversal, ChooseSemantics.SWITCH);
    }

    public ChooseStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?> predicateTraversal,
                      final Traversal.Admin<S, E> trueChoice, final Traversal.Admin<S, E> falseChoice) {
        this(traversal, (Traversal.Admin<S, M>) predicateTraversal.addStep(new HasNextStep<>(predicateTraversal)), ChooseSemantics.IF_THEN);
        this.addChildOption((M) Boolean.TRUE, trueChoice);
        this.addChildOption((M) Boolean.FALSE, falseChoice);
    }

    /**
     * Step metadata that informs on how {@code choose()} is configured to work.
     */
    public ChooseSemantics getChooseSemantics() {
        return chooseSemantics;
    }

    /**
     * Adds a child option to this ChooseStep.This method overrides the parent implementation to provide special
     * handling for {@link Pick} tokens.
     */
    @Override
    public void addChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (pickToken instanceof Pick) {
            if (Pick.any.equals(pickToken))
                throw new IllegalArgumentException("Choose step can not have an any-option as only one option per traverser is allowed");

            // trying to maintain semantics where the first match wins. for unproductive, that means, we
            // have to get rid of the default one added in the constructor, but not add future ones. for
            // none that means we only add the first but not the future ones.
            if (Pick.unproductive.equals(pickToken) && hasDefaultUnproductive) {
                this.hasDefaultUnproductive = false;
                this.traversalPickOptions.remove(pickToken);
                super.addChildOption(pickToken, traversalOption);
            } else if (Pick.none.equals(pickToken) && hasDefaultNone) {
                this.hasDefaultNone = false;
                this.traversalPickOptions.remove(pickToken);
                super.addChildOption(pickToken, traversalOption);
            } else if (pickToken == Pick.none && !this.traversalPickOptions.containsKey(Pick.none)) {
                super.addChildOption(pickToken, traversalOption);
            }
        } else {
            super.addChildOption(pickToken, traversalOption);
        }
    }

    /**
     * Selects which branches to execute based on the choice value.
     */
    @Override
    protected List<Traversal.Admin<S, E>> pickBranches(final M choice) {
        final List<Traversal.Admin<S, E>> branches = super.pickBranches(choice);
        return branches != null ? branches.subList(0, 1) : null;
    }
}
