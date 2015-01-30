package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.traversal.TraversalHelper;
import com.tinkerpop.gremlin.process.util.traversal.TraversalRing;
import com.tinkerpop.gremlin.process.util.traversal.TraversalUtil;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements TraversalHolder, EngineDependent {

    protected TraversalRing<Object, Object> traversalRing = new TraversalRing<>();
    private final List<String> selectLabels;
    private final boolean wasEmpty;
    private boolean requiresPaths = false;
    protected Function<Traverser<S>, Map<String, E>> selectFunction;

    public SelectStep(final Traversal traversal, final String... selectLabels) {
        super(traversal);
        this.wasEmpty = selectLabels.length == 0;
        this.selectLabels = this.wasEmpty ? TraversalHelper.getLabelsUpTo(this, this.traversal.asAdmin()) : Arrays.asList(selectLabels);
        SelectStep.generateFunction(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.requiresPaths = traversalEngine.equals(TraversalEngine.COMPUTER) ?
                TraversalHelper.getLabelsUpTo(this, this.traversal.asAdmin()).stream().filter(this.selectLabels::contains).findAny().isPresent() :
                TraversalHelper.getStepsUpTo(this, this.traversal.asAdmin()).stream()
                        .filter(step -> step instanceof CollectingBarrierStep)
                        .filter(step -> TraversalHelper.getLabelsUpTo(step, this.traversal.asAdmin()).stream().filter(this.selectLabels::contains).findAny().isPresent()
                                || (step.getLabel().isPresent() && this.selectLabels.contains(step.getLabel().get()))) // TODO: get rid of this (there is a test case to check it)
                        .findAny().isPresent();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabels, this.traversalRing);
    }

    @Override
    public SelectStep<S, E> clone() throws CloneNotSupportedException {
        final SelectStep<S, E> clone = (SelectStep<S, E>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            final Traversal.Admin<Object, Object> traversalClone = traversal.clone();
            clone.traversalRing.addTraversal(traversalClone);
            clone.executeTraversalOperations(traversalClone, TYPICAL_LOCAL_OPERATIONS);
        }
        SelectStep.generateFunction(clone);
        return clone;
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalTraversals() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalTraversal(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal((Traversal.Admin<Object, Object>) selectTraversal);
        this.executeTraversalOperations(selectTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
        if (this.requiresPaths)
            requirements.add(TraverserRequirement.PATH);
        requirements.add(TraverserRequirement.OBJECT);
        requirements.add(TraverserRequirement.PATH_ACCESS);
        return requirements;
    }

    //////////////////////

    private static final <S, E> void generateFunction(final SelectStep<S, E> selectStep) {
        selectStep.selectFunction = traverser -> {
            final S start = traverser.get();
            final Map<String, E> bindings = new LinkedHashMap<>();

            ////// PROCESS STEP BINDINGS
            final Path path = traverser.path();
            selectStep.selectLabels.forEach(label -> {
                if (path.hasLabel(label))
                    bindings.put(label, (E) TraversalUtil.function(path.<Object>get(label), selectStep.traversalRing.next()));
            });

            ////// PROCESS MAP BINDINGS
            if (start instanceof Map) {
                if (selectStep.wasEmpty)
                    ((Map<String, Object>) start).forEach((k, v) -> bindings.put(k, (E) TraversalUtil.function(v, selectStep.traversalRing.next())));
                else
                    selectStep.selectLabels.forEach(label -> {
                        if (((Map) start).containsKey(label)) {
                            bindings.put(label, (E) TraversalUtil.function(((Map) start).get(label), selectStep.traversalRing.next()));
                        }
                    });
            }

            selectStep.traversalRing.reset();
            return bindings;
        };
        selectStep.setFunction(selectStep.selectFunction);
    }
}
