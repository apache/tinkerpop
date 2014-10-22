package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalHelper {

    public static boolean isLabeled(final Step<?, ?> step) {
        return !Graph.System.isSystem(step.getLabel());
    }

    public static boolean isLabeled(final String label) {
        return !Graph.System.isSystem(label);
    }

    public static boolean isReversible(final Traversal<?, ?> traversal) {
        return !traversal.getSteps().stream().filter(step -> !(step instanceof Reversible)).findAny().isPresent();
    }

    public static <C extends Step> Optional<C> getLastStep(final Traversal<?, ?> traversal, final Class<C> classToGet) {
        final List<C> steps = (List) traversal.getSteps().stream().filter(step -> classToGet.isAssignableFrom(step.getClass())).collect(Collectors.toList());
        return steps.size() == 0 ? Optional.empty() : Optional.of(steps.get(steps.size() - 1));
    }

    public static <S, E> Step<S, E> getStep(final String label, final Traversal<?, ?> traversal) {
        return traversal.getSteps().stream()
                .filter(step -> label.equals(step.getLabel()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("The provided step label does not exist: " + label));
    }

    public static boolean hasLabel(final String label, final Traversal<?, ?> traversal) {
        return traversal.getSteps().stream()
                .filter(step -> label.equals(step.getLabel()))
                .findAny().isPresent();
    }

    public static List<String> getLabels(final Traversal<?, ?> traversal) {
        final List<String> labels = new ArrayList<>();
        for (final Step step : traversal.getSteps()) {
            final String label = step.getLabel();
            if (isLabeled(label)) {
                labels.add(label);
            }
        }
        return labels;
    }

    public static List<String> getLabelsUpTo(final Step<?, ?> step, final Traversal<?, ?> traversal) {
        final List<String> labels = new ArrayList<>();
        for (final Step temp : traversal.getSteps()) {
            if (temp == step) break;
            final String label = temp.getLabel();
            if (isLabeled(label)) {
                labels.add(label);
            }
        }
        return labels;
    }

    public static List<Step<?, ?>> getStepsUpTo(final Step<?, ?> step, final Traversal<?, ?> traversal) {
        final List<Step<?, ?>> steps = new ArrayList<>();
        for (final Step temp : traversal.getSteps()) {
            if (temp == step) break;
            steps.add(temp);
        }
        return steps;
    }

    public static <S, E> Step<S, ?> getStart(final Traversal<S, E> traversal) {
        return traversal.getSteps().get(0);
    }

    public static <S, E> Step<?, E> getEnd(final Traversal<S, E> traversal) {
        return traversal.getSteps().get(traversal.getSteps().size() - 1);
    }

    public static boolean areEqual(final Iterator a, final Iterator b) {
        while (a.hasNext() || b.hasNext()) {
            if (a.hasNext() != b.hasNext())
                return false;
            if (!a.next().equals(b.next()))
                return false;
        }
        return true;
    }

    public static void iterate(final Iterator iterator) {
        try {
            while (true) {
                iterator.next();
            }
        } catch (final NoSuchElementException ignored) {

        }
    }

    public static int removeStep(final Step<?, ?> step, final Traversal<?, ?> traversal) {
        final int stepIndex = traversal.getSteps().indexOf(step);
        traversal.getSteps().remove(step);
        reLabelSteps(traversal);
        reLinkSteps(traversal);
        return stepIndex;
    }

    public static void removeStep(final int index, final Traversal<?, ?> traversal) {
        TraversalHelper.removeStep(traversal.getSteps().get(index), traversal);
    }

    public static void insertStep(final Step step, final int index, final Traversal<?, ?> traversal) {
        traversal.getSteps().add(index, step);
        reLabelSteps(traversal);
        reLinkSteps(traversal);
    }

    public static void insertStep(final Step step, final Traversal<?, ?> traversal) {
        traversal.getSteps().add(step);
        reLabelSteps(traversal);
        reLinkSteps(traversal);
    }

    public static void insertBeforeStep(final Step<?, ?> step, final Step<?, ?> afterStep, final Traversal<?, ?> traversal) {
        TraversalHelper.insertStep(step, traversal.getSteps().indexOf(afterStep), traversal);
    }

    public static void insertAfterStep(final Step<?, ?> step, final Step<?, ?> beforeStep, final Traversal<?, ?> traversal) {
        TraversalHelper.insertStep(step, traversal.getSteps().indexOf(beforeStep) + 1, traversal);
    }

    public static void replaceStep(final Step<?, ?> removeStep, final Step<?, ?> insertStep, final Traversal<?, ?> traversal) {
        int index = TraversalHelper.removeStep(removeStep, traversal);
        TraversalHelper.insertStep(insertStep, index, traversal);
    }

    private static void reLabelSteps(final Traversal<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            if (!TraversalHelper.isLabeled(steps.get(i)))
                steps.get(i).setLabel(Graph.System.system(Integer.toString(i)));
        }
    }

    private static void reLinkSteps(final Traversal<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            final Step previousStep = i > 0 ? steps.get(i - 1) : null;
            final Step currentStep = steps.get(i);
            final Step nextStep = i < steps.size() - 1 ? steps.get(i + 1) : null;
            currentStep.setPreviousStep(null != previousStep ? previousStep : EmptyStep.instance());
            currentStep.setNextStep(null != nextStep ? nextStep : EmptyStep.instance());
        }
    }

    public static String makeStepString(final Step<?, ?> step, final Object... arguments) {
        final StringBuilder builder = new StringBuilder(step.getClass().getSimpleName());
        if (arguments.length > 0) {
            builder.append("(");
            for (int i = 0; i < arguments.length; i++) {
                if (i > 0) builder.append(",");
                builder.append(arguments[i]);
            }
            builder.append(")");
        }
        if (TraversalHelper.isLabeled(step))
            builder.append("@").append(step.getLabel());
        return builder.toString();
    }

    public static String makeTraversalString(final Traversal<?, ?> traversal) {
        final List<Step> temp = new ArrayList<>();
        Step currentStep = TraversalHelper.getStart(traversal);
        while (!(currentStep instanceof EmptyStep)) {
            temp.add(currentStep);
            currentStep = currentStep.getNextStep();
        }
        return temp.toString();
    }

    public static boolean trackPaths(final Traversal<?, ?> traversal) {
        return traversal.getSteps().stream()
                .filter(step -> step instanceof PathConsumer && ((PathConsumer) step).requiresPaths())
                .findAny()
                .isPresent();
    }

    public static List<Step> isolateSteps(final Step<?, ?> from, final Step<?, ?> to) {
        final List<Step> steps = new ArrayList<>();
        Step step = from.getNextStep();
        while (step != to && !(step instanceof EmptyStep)) {
            steps.add(step);
            step = step.getNextStep();
        }
        if (step instanceof EmptyStep)
            steps.clear();
        return steps;
    }

    public static <S extends Step> List<S> getStepsOfClass(final Class<S> stepClass, final Traversal<?, ?> traversal) {
        return (List) traversal.getSteps().stream().filter(step -> step.getClass().equals(stepClass)).collect(Collectors.toList());
    }

    public static boolean hasStepOfClass(final Class<? extends Step> stepClass, final Traversal<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step.getClass().equals(stepClass)) {
                return true;
            }
        }
        return false;
    }

    /*public static void printTraversalChain(final Traversal<?,?> traversal) {
        Step step = TraversalHelper.getStart(traversal);
        while (!step.equals(EmptyStep.instance())) {
            System.out.println(step);
            step = step.getNextStep();
        }
    }*/

    public static int relativeLabelDirection(Step<?, ?> step, final String label) {
        if (label.equals(step.getLabel()))
            return 0;
        while (!(step instanceof EmptyStep)) {
            if (label.equals(step.getLabel()))
                return 1;
            step = step.getNextStep();
        }
        return -1;
    }

    public static void verifySideEffectKeyIsNotAStepLabel(final String key, final Traversal<?, ?> traversal) {
        if (TraversalHelper.hasLabel(key, traversal))
            throw new IllegalArgumentException("The provided side effect key is already used as a step label: " + key);
    }

    public static void verifyStepLabelIsNotASideEffectKey(final String label, final Traversal<?, ?> traversal) {
        if (traversal.sideEffects().exists(label))
            throw new IllegalArgumentException("The provided step label is already used as a side effect key: " + label);
    }

    public static void verifyStepLabelIsNotAlreadyAStepLabel(final String label, final Traversal<?, ?> traversal) {
        if (TraversalHelper.hasLabel(label, traversal))
            throw new IllegalArgumentException("The provided step label is already being used as a step label: " + label);
    }

    public static <S> void addToCollection(final Collection<S> collection, final S s, final long bulk) {
        if (collection instanceof BulkSet) {
            ((BulkSet<S>) collection).add(s, bulk);
        } else if (collection instanceof Set) {
            collection.add(s);
        } else {
            for (long i = 0; i < bulk; i++) {
                collection.add(s);
            }
        }
    }

    public static <S> void addToCollectionUnrollIterator(final Collection<S> collection, final S s, final long bulk) {
        if (s instanceof Iterator) {
            ((Iterator<S>) s).forEachRemaining(r -> addToCollection(collection, r, bulk));
        } else if (s instanceof Iterable) {
            ((Iterable<S>) s).forEach(r -> addToCollection(collection, r, bulk));
        } else {
            addToCollection(collection, s, bulk);
        }
    }

    /**
     * Returns the name of <i>step</i> truncated to <i>maxLength</i>. An ellipses is appended when the name exceeds
     * <i>maxLength</i>.
     *
     * @param step
     * @param maxLength Includes the 3 "..." characters that will be appended when the length of the name exceeds
     *                  maxLength.
     * @return short step name.
     */
    public static String getShortName(final Step step, final int maxLength) {
        final String name = step.toString();
        if (name.length() > maxLength)
            return name.substring(0, maxLength - 3) + "...";
        return name;
    }
}
