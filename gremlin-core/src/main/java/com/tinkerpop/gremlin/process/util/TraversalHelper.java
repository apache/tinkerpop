package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalHelper {

    private static final String UNDERSCORE = "_";

    public static boolean isLabeled(final Step step) {
        return !step.getAs().startsWith(UNDERSCORE);
    }

    public static boolean isLabeled(final String as) {
        return !as.startsWith(UNDERSCORE);
    }

    public static boolean isReversible(final Traversal traversal) {
        return !traversal.getSteps().stream().filter(step -> !(step instanceof Reversible)).findFirst().isPresent();
    }

    public static <C extends Step> Optional<C> getLastStep(final Traversal traversal, final Class<C> classToGet) {
        final List<C> steps = (List) traversal.getSteps().stream().filter(step -> classToGet.isAssignableFrom(step.getClass())).collect(Collectors.<C>toList());
        return steps.size() == 0 ? Optional.empty() : Optional.of(steps.get(steps.size() - 1));
    }

    public static Step getLastStep(final Traversal traversal) {
        return (Step) traversal.getSteps().get(traversal.getSteps().size() - 1);
    }

    public static <S, E> Step<S, E> getAs(final String as, final Traversal<?, ?> traversal) {
        return (Step) traversal.getSteps().stream()
                .filter(p -> as.equals(p.getAs()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("The provided name does not exist: " + as));
    }

    public static boolean asExists(final String as, final Traversal<?, ?> traversal) {
        return traversal.getSteps().stream()
                .filter(p -> as.equals(p.getAs()))
                .findFirst().isPresent();
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

    public static boolean hasNextIteration(final Iterator iterator) {
        if (iterator.hasNext()) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            return true;
        } else {
            return false;
        }
    }

    public static void iterate(final Iterator iterator) {
        try {
            while (true) {
                iterator.next();
            }
        } catch (final NoSuchElementException e) {

        }
    }

    public static int removeStep(final Step step, final Traversal traversal) {
        final int stepIndex = traversal.getSteps().indexOf(step);
        traversal.getSteps().remove(step);
        reLabelSteps(traversal);
        reLinkSteps(traversal);
        return stepIndex;
    }

    public static void removeStep(final int index, final Traversal traversal) {
        TraversalHelper.removeStep((Step) traversal.getSteps().get(index), traversal);
    }

    public static void insertStep(final Step step, final int index, final Traversal traversal) {
        traversal.getSteps().add(index, step);
        reLabelSteps(traversal);
        reLinkSteps(traversal);
    }

    public static void replaceStep(final Step removeStep, final Step insertStep, final Traversal traversal) {
        int index = TraversalHelper.removeStep(removeStep, traversal);
        TraversalHelper.insertStep(insertStep, index, traversal);
    }

    private static void reLabelSteps(final Traversal traversal) {
        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            if (!TraversalHelper.isLabeled(steps.get(i)))
                steps.get(i).setAs(UNDERSCORE + i);
        }
    }

    private static void reLinkSteps(final Traversal traversal) {
        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            final Step previousStep = i > 0 ? steps.get(i - 1) : null;
            final Step currentStep = steps.get(i);
            final Step nextStep = i < steps.size() - 1 ? steps.get(i + 1) : null;
            currentStep.setPreviousStep(null != previousStep ? previousStep : EmptyStep.instance());
            currentStep.setNextStep(null != nextStep ? nextStep : EmptyStep.instance());
        }
    }

    public static String makeStepString(final Step step, final Object... arguments) {
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
            builder.append("@").append(step.getAs());
        return builder.toString();
    }
}
