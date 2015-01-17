package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalHelper {

    public static boolean isReversible(final Traversal<?, ?> traversal) {
        return !traversal.asAdmin().getSteps().stream().filter(step -> !(step instanceof Reversible)).findAny().isPresent();
    }

    public static <C extends Step> Optional<C> getLastStep(final Traversal<?, ?> traversal, final Class<C> classToGet) {
        final List<C> steps = (List) traversal.asAdmin().getSteps().stream().filter(step -> classToGet.isAssignableFrom(step.getClass())).collect(Collectors.toList());
        return steps.size() == 0 ? Optional.empty() : Optional.of(steps.get(steps.size() - 1));
    }

    public static <S, E> Step<S, E> getStep(final String label, final Traversal<?, ?> traversal) {
        return traversal.asAdmin().getSteps().stream()
                .filter(step -> step.getLabel().isPresent())
                .filter(step -> label.equals(step.getLabel().get()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("The provided step label does not exist: " + label));
    }

    public static <S, E> Step<S, E> getStepById(final String id, final Traversal<?, ?> traversal) {
        return traversal.asAdmin().getSteps().stream()
                .filter(step -> id.equals(step.getId()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("The provided step id does not exist: " + id));
    }


    public static <S, E> Optional<Step<S, E>> getStepByIdRecurssively(final String id, final Traversal<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (step.getId().equals(id))
                return Optional.of((Step) step);
            else if (step instanceof TraversalHolder) {
                for (final Traversal<?, ?> nest : ((TraversalHolder<?, ?>) step).getTraversals()) {
                    final Optional<Step<S, E>> optional = TraversalHelper.getStepByIdRecurssively(id, nest);
                    if (optional.isPresent())
                        return optional;
                }
            }
        }
        return Optional.empty();
    }

    public static boolean hasLabel(final String label, final Traversal<?, ?> traversal) {
        return traversal.asAdmin().getSteps().stream()
                .filter(step -> step.getLabel().isPresent())
                .filter(step -> label.equals(step.getLabel().get()))
                .findAny().isPresent();
    }

    public static List<String> getLabelsUpTo(final Step<?, ?> step, final Traversal<?, ?> traversal) {
        final List<String> labels = new ArrayList<>();
        for (final Step<?, ?> temp : traversal.asAdmin().getSteps()) {
            if (temp == step) break;
            temp.getLabel().ifPresent(labels::add);
        }
        return labels;
    }

    public static List<Step<?, ?>> getStepsUpTo(final Step<?, ?> step, final Traversal<?, ?> traversal) {
        final List<Step<?, ?>> steps = new ArrayList<>();
        for (final Step temp : traversal.asAdmin().getSteps()) {
            if (temp == step) break;
            steps.add(temp);
        }
        return steps;
    }

    public static <S, E> Step<S, ?> getStart(final Traversal<S, E> traversal) {
        return traversal.asAdmin().getSteps().size() == 0 ? EmptyStep.instance() : traversal.asAdmin().getSteps().get(0);
    }

    public static <S, E> Step<?, E> getEnd(final Traversal<S, E> traversal) {
        return traversal.asAdmin().getSteps().size() == 0 ? EmptyStep.instance() : traversal.asAdmin().getSteps().get(traversal.asAdmin().getSteps().size() - 1);
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

    public static Step<?, ?> insertTraversal(final int insertIndex, final Traversal<?, ?> insertTraversal, final Traversal<?, ?> traversal) {
        if (0 == traversal.asAdmin().getSteps().size()) {
            for (final Step insertStep : insertTraversal.asAdmin().getSteps()) {
                traversal.asAdmin().addStep(insertStep);
            }
            return TraversalHelper.getEnd(traversal);
        } else {
            Step currentStep = traversal.asAdmin().getSteps().get(insertIndex);
            for (final Step insertStep : insertTraversal.asAdmin().getSteps()) {
                TraversalHelper.insertAfterStep(insertStep, currentStep, traversal);
                currentStep = insertStep;
            }
            return currentStep;
        }
    }

    public static Step<?, ?> insertTraversal(final Step<?, ?> previousStep, final Traversal insertTraversal, final Traversal<?, ?> traversal) {
        return TraversalHelper.insertTraversal(traversal.asAdmin().getSteps().indexOf(previousStep), insertTraversal, traversal);
    }

    public static void insertBeforeStep(final Step<?, ?> step, final Step<?, ?> afterStep, final Traversal<?, ?> traversal) {
        traversal.asAdmin().addStep(traversal.asAdmin().getSteps().indexOf(afterStep), step);
    }

    public static void insertAfterStep(final Step<?, ?> step, final Step<?, ?> beforeStep, final Traversal<?, ?> traversal) {
        traversal.asAdmin().addStep(traversal.asAdmin().getSteps().indexOf(beforeStep) + 1, step);
    }

    public static void replaceStep(final Step<?, ?> removeStep, final Step<?, ?> insertStep, final Traversal<?, ?> traversal) {
        traversal.asAdmin().addStep(traversal.asAdmin().getSteps().indexOf(removeStep), insertStep);
        traversal.asAdmin().removeStep(removeStep);
    }

    public static void reLinkSteps(final Traversal<?, ?> traversal) {
        final List<Step> steps = traversal.asAdmin().getSteps();
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
        final List<String> strings = Stream.of(arguments)
                .filter(o -> null != o)
                .filter(o -> {
                    if (o instanceof FunctionRing) {
                        return ((FunctionRing) o).size() > 0;
                    } else if (o instanceof Collection) {
                        return ((Collection) o).size() > 0;
                    } else if (o instanceof Map) {
                        return ((Map) o).size() > 0;
                    } else {
                        return true;
                    }
                })
                .map(o -> {
                    final String string = o.toString();
                    return string.contains("$") ? "lambda" : string;
                }).filter(o -> !Graph.Hidden.isHidden(o))
                .collect(Collectors.toList());
        if (strings.size() > 0) {
            builder.append("(");
            builder.append(String.join(",", strings));
            builder.append(")");
        }
        step.getLabel().ifPresent(label -> builder.append("@").append(label));
        //builder.append("^").append(step.getId());
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
        return (List) traversal.asAdmin().getSteps().stream().filter(step -> step.getClass().equals(stepClass)).collect(Collectors.toList());
    }

    public static <S extends Step> List<S> getStepsOfAssignableClass(final Class stepClass, final Traversal<?, ?> traversal) {
        return (List) traversal.asAdmin().getSteps().stream().filter(step -> stepClass.isAssignableFrom(step.getClass())).collect(Collectors.toList());
    }

    public static <S extends Step> List<S> getStepsOfAssignableClassRecurssively(final Class stepClass, final Traversal<?, ?> traversal) {
        final List<S> list = new ArrayList<>();
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                list.add((S) step);
            if (step instanceof TraversalHolder) {
                for (final Traversal<?, ?> nest : ((TraversalHolder<?, ?>) step).getTraversals()) {
                    list.addAll(TraversalHelper.getStepsOfAssignableClassRecurssively(stepClass, nest));
                }
            }
        }
        return list;
    }

    public static boolean hasStepOfClass(final Class<? extends Step> stepClass, final Traversal<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (step.getClass().equals(stepClass)) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasStepOfAssignableClass(final Class superClass, final Traversal<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (superClass.isAssignableFrom(step.getClass())) {
                return true;
            }
        }
        return false;
    }

    public static void verifySideEffectKeyIsNotAStepLabel(final String key, final Traversal<?, ?> traversal) {
        if (TraversalHelper.hasLabel(key, traversal))
            throw new IllegalArgumentException("The provided side effect key is already used as a step label: " + key);
    }

    public static void verifyStepLabelIsNotASideEffectKey(final String label, final Traversal<?, ?> traversal) {
        if (traversal.asAdmin().getSideEffects().exists(label))
            throw new IllegalArgumentException("The provided step label is already used as a side effect key: " + label);
    }

    public static void verifyStepLabelIsNotAlreadyAStepLabel(final String label, final Traversal<?, ?> traversal) {
        if (TraversalHelper.hasLabel(label, traversal))
            throw new IllegalArgumentException("The provided step label is already being used as a step label: " + label);
    }

    public static void verifyStepLabelIsNotHidden(final String label) {
        if (Graph.Hidden.isHidden(label))
            throw new IllegalArgumentException("The provided step label can not be hidden: " + label);
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

    public static Set<TraverserRequirement> getRequirements(final Traversal<?, ?> traversal) {
        final Set<TraverserRequirement> requirements = traversal.asAdmin().getSteps().stream()
                .flatMap(step -> ((Step<?, ?>) step).getRequirements().stream())
                .collect(Collectors.toSet());
        if (traversal.asAdmin().getSideEffects().keys().size() > 0)
            requirements.add(TraverserRequirement.SIDE_EFFECTS);
        if (traversal.asAdmin().getSideEffects().getSackInitialValue().isPresent())
            requirements.add(TraverserRequirement.SACK);
        return requirements;
    }

    private static Triplet<Integer, Integer, String> getYZParentId(final Traversal<?, ?> traversal) {
        int y = -1;
        int z = -1;
        String parentId = null;
        Traversal current = traversal;
        while (!(current instanceof EmptyTraversal)) {
            y++;
            final TraversalHolder<?, ?> holder = current.asAdmin().getTraversalHolder();
            if (null == parentId && !(holder instanceof EmptyStep)) {
                parentId = holder.asStep().getId();
            }
            if (z == -1) {
                for (int i = 0; i < holder.getTraversals().size(); i++) {
                    if (holder.getTraversals().get(i) == current) {
                        z = i;
                    }
                }
            }
            current = holder.asStep().getTraversal();
        }
        return Triplet.with(y, z == -1 ? 0 : z, null == parentId ? "" : parentId);
    }

    public static void reIdSteps(final StepPosition stepPosition, final Traversal<?, ?> traversal) {
        stepPosition.reset();
        final Triplet<Integer, Integer, String> yzParentId = TraversalHelper.getYZParentId(traversal);
        stepPosition.y = yzParentId.getValue0();
        stepPosition.z = yzParentId.getValue1();
        stepPosition.parentId = yzParentId.getValue2();
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            step.setId(stepPosition.nextXId());
        }
    }

    public static Traversal<?, ?> getRootTraversal(Traversal<?, ?> traversal) {
        while (!((traversal.asAdmin().getTraversalHolder()) instanceof EmptyStep)) {
            traversal = traversal.asAdmin().getTraversalHolder().asStep().getTraversal();
        }
        return traversal;
    }
}
