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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Property-based tests for addLabel() and dropLabel() traversal steps.
 * Uses randomized inputs to validate universal correctness properties.
 */
public class LabelMutationPropertyTest {

    private static final Logger logger = LoggerFactory.getLogger(LabelMutationPropertyTest.class);
    private static final int ITERATIONS = 100;
    private static final String LABEL_CHARS = "abcdefghijklmnopqrstuvwxyz";

    private Graph graph;
    private GraphTraversalSource g;
    private Random random;

    @Before
    public void setup() {
        graph = TinkerGraph.open();
        g = graph.traversal();
        random = new Random(42); // deterministic seed for reproducibility
    }

    @After
    public void tearDown() throws Exception {
        graph.close();
    }

    private String randomLabel() {
        final int len = 1 + random.nextInt(8);
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(LABEL_CHARS.charAt(random.nextInt(LABEL_CHARS.length())));
        }
        return sb.toString();
    }

    private Set<String> randomLabelSet(final int minSize, final int maxSize) {
        final int size = minSize + random.nextInt(maxSize - minSize + 1);
        final Set<String> labels = new HashSet<>();
        while (labels.size() < size) {
            labels.add(randomLabel());
        }
        return labels;
    }

    /**
     * Property 4: AddLabel idempotence.
     * For any vertex and any label L, if L is already in the vertex's label set,
     * calling addLabel(L) shall result in the label set being unchanged.
     */
    @Test
    public void shouldBeIdempotentWhenAddingExistingLabel() {
        for (int i = 0; i < ITERATIONS; i++) {
            final Set<String> initialLabels = randomLabelSet(1, 5);
            final Vertex v = g.addV(initialLabels.iterator().next()).next();
            // add remaining labels
            for (final String l : initialLabels) {
                g.V(v).addLabel(l).iterate();
            }

            // pick a label already present
            final List<String> labelList = new ArrayList<>(v.labels());
            final String existingLabel = labelList.get(random.nextInt(labelList.size()));
            final Set<String> beforeAdd = new HashSet<>(v.labels());

            // add it again - should be idempotent
            g.V(v).addLabel(existingLabel).iterate();
            final Set<String> afterAdd = new HashSet<>(v.labels());

            assertThat("Iteration " + i + ": addLabel should be idempotent for label '" + existingLabel + "'",
                    afterAdd, is(beforeAdd));
        }
    }

    /**
     * Property 6: DropLabels removes all labels and applies default.
     * For any TinkerVertex with any number of labels, calling dropLabels()
     * shall result in the vertex having only the default label "vertex".
     */
    @Test
    public void shouldApplyDefaultLabelAfterDroppingAllLabels() {
        for (int i = 0; i < ITERATIONS; i++) {
            final Set<String> initialLabels = randomLabelSet(1, 5);
            final Vertex v = g.addV(initialLabels.iterator().next()).next();
            for (final String l : initialLabels) {
                g.V(v).addLabel(l).iterate();
            }

            // drop all labels
            g.V(v).dropLabels().iterate();

            assertThat("Iteration " + i + ": after dropLabels(), vertex should have exactly one label",
                    v.labels(), hasSize(1));
            assertThat("Iteration " + i + ": after dropLabels(), vertex should have default label",
                    v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL));
        }
    }

    /**
     * Property 8: DropLabel non-existent labels no-op.
     * For any vertex and any label L not present in the vertex's label set,
     * calling dropLabel(L) shall leave the label set unchanged.
     */
    @Test
    public void shouldBeNoOpWhenDroppingNonExistentLabel() {
        for (int i = 0; i < ITERATIONS; i++) {
            final Set<String> initialLabels = randomLabelSet(1, 5);
            final Vertex v = g.addV(initialLabels.iterator().next()).next();
            for (final String l : initialLabels) {
                g.V(v).addLabel(l).iterate();
            }

            // generate a label guaranteed not to be in the set
            String nonExistent = randomLabel();
            while (v.labels().contains(nonExistent)) {
                nonExistent = randomLabel();
            }

            final Set<String> beforeDrop = new HashSet<>(v.labels());
            g.V(v).dropLabel(nonExistent).iterate();
            final Set<String> afterDrop = new HashSet<>(v.labels());

            assertThat("Iteration " + i + ": dropLabel of non-existent label '" + nonExistent + "' should be no-op",
                    afterDrop, is(beforeDrop));
        }
    }

    /**
     * Property 12: ValueMap/ElementMap multilabel configuration.
     * For any element with labels L, valueMap(true).with(WithOptions.multilabel) shall return
     * labels as Set&lt;String&gt; equal to L, and without the config shall return a single String from L.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void shouldReturnCorrectLabelTypeBasedOnMultilabelConfig() {
        for (int i = 0; i < ITERATIONS; i++) {
            final Set<String> initialLabels = randomLabelSet(1, 4);
            final Vertex v = g.addV(initialLabels.iterator().next()).next();
            for (final String l : initialLabels) {
                g.V(v).addLabel(l).iterate();
            }

            // with multilabel config: should return Set<String>
            final Map<Object, Object> mapWithConfig = g.V(v).valueMap(true)
                    .with(WithOptions.multilabel).next();
            final Object labelWithConfig = mapWithConfig.get(T.label);
            assertThat("Iteration " + i + ": with multilabel config, label should be a Set",
                    labelWithConfig, instanceOf(Set.class));
            assertThat("Iteration " + i + ": with multilabel config, labels should match",
                    (Set<String>) labelWithConfig, is(v.labels()));

            // without multilabel config: should return single String
            final Map<Object, Object> mapWithoutConfig = g.V(v).valueMap(true).next();
            final Object labelWithoutConfig = mapWithoutConfig.get(T.label);
            assertThat("Iteration " + i + ": without multilabel config, label should be a String",
                    labelWithoutConfig, instanceOf(String.class));
            assertThat("Iteration " + i + ": without multilabel config, label should be in vertex labels",
                    v.labels().contains((String) labelWithoutConfig), is(true));
        }
    }
}
