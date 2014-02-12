package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Step;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinHelper {

    private static final String UNDERSCORE = "_";

    public static boolean isLabeled(final Step step) {
        return !step.getAs().startsWith(UNDERSCORE);
    }

    public static boolean isLabeled(final String as) {
        return !as.startsWith(UNDERSCORE);
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

    public static void removePipe(final Step step, final Traversal traversal) {
        final List<Step> steps = traversal.getSteps();
        final int index = steps.indexOf(step);
        if (index - 1 >= 0 && index + 1 < steps.size()) {
            steps.get(index - 1).setNextStep(steps.get(index + 1));
            steps.get(index + 1).setPreviousStep(steps.get(index - 1));
        }
        steps.remove(step);
    }

    public static void insertPipe(final Step step, final int index, final Traversal traversal) {
        final List<Step> steps = traversal.getSteps();
        final Step leftStep = steps.get(index - 1);
        final Step rightStep = steps.get(index);
        leftStep.setNextStep(step);
        step.setPreviousStep(leftStep);
        step.setNextStep(rightStep);
        rightStep.setPreviousStep(step);
        steps.add(index, step);
    }

    public static String makePipeString(final Step step, final Object... arguments) {
        final StringBuilder builder = new StringBuilder(step.getClass().getSimpleName());
        if (arguments.length > 0) {
            builder.append("(");
            for (int i = 0; i < arguments.length; i++) {
                if (i > 0) builder.append(",");
                builder.append(arguments[i]);
            }
            builder.append(")");
        }
        if (GremlinHelper.isLabeled(step))
            builder.append("@").append(step.getAs());
        return builder.toString();
    }
}
