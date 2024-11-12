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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DedupLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IndexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SampleLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TailLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalSelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

/**
 * Output Arity Predictor for Tinkerpop steps
 */
public class StepOutputArityPredictor {
    public enum Arity {

        /**
         * This enum value tells that the output arity of the step could be singular at best.
         * For example: HasStep.
         * HasStep can have atmost 1 output coming out after its execution. But it would never increase the cardinality
         * of the output
         */
        MAY_BE_SINGULAR(1),
        /**
         * This enum value tells that the output arity of the step will be singular definitely.
         * For examples: IdentityStep, IDStep, ValueMapStep.
         */
        DEFINITELY_SINGULAR(1),
        /**
         * This enum value tells that the output arity of the step is multiple.
         * For example: out(), in() unfold etc.
         */
        MULTI(2);
        private final int priority;

        Arity(final int inputPriority){
            this.priority = inputPriority;
        }

        public int getPriority() {
            return this.priority;
        }

        public Arity computeArity(final Arity operandArity) {
            final Arity inputArity = this;
            if (inputArity.getPriority() * operandArity.getPriority() > 1) {
                return MULTI;
            } else if (inputArity.equals(MAY_BE_SINGULAR) || operandArity.equals(MAY_BE_SINGULAR)) {
                return MAY_BE_SINGULAR;
            }
            return DEFINITELY_SINGULAR;
        }
    }

    private static Arity wouldEdgeVertexStepHaveSingleResult(final Step<?, ?> step) {
        final EdgeVertexStep edgeVertexStep = (EdgeVertexStep) step;
        if (edgeVertexStep.getDirection().equals(Direction.BOTH)) {
            return Arity.MULTI;
        }
        return Arity.MAY_BE_SINGULAR;
    }

    private static Arity getOutputArityBehaviorForValueTraversal(final Traversal traversal) {
        // If it's a value traversal and value seeked is 'Id' then it is definitely SINGULAR otherwise multi
        final ValueTraversal valueTraversal = (ValueTraversal) traversal;
        if (((ValueTraversal<?, ?>) traversal).getPropertyKey().equals(T.id.getAccessor())) {
            return Arity.DEFINITELY_SINGULAR;
        } else if (((ValueTraversal<?, ?>) traversal).getPropertyKey().equals(T.label.getAccessor())) {
            return STEP_TO_ARITY_FUNCTION_MAP.get(LabelStep.class.getSimpleName()).apply(null);
        }
        return Arity.MULTI;
    }

    private static Arity getOutputArityBehaviorForTraversalMapStep(final Step step) {
        if (!(step instanceof TraversalMapStep)) {
            throw new IllegalArgumentException(String.format("Expected TraversalMapStep but got '%s'", step.getClass().getSimpleName()));
        }
        final TraversalMapStep mapStep = (TraversalMapStep) step;
        return getOutputArity((Traversal<?, ?>) mapStep.getLocalChildren().get(0));

    }

    private static Arity getOutputArityBehaviorForTokenTraversal(final Traversal traversal) {
        // if its a token traversal then
        // id is Definitely singular

        final TokenTraversal tokenTraversal = (TokenTraversal) traversal;
        if (tokenTraversal.getToken().equals(T.id)) {
            return Arity.DEFINITELY_SINGULAR;
        } else if (tokenTraversal.getToken().equals(T.label)) {
            return STEP_TO_ARITY_FUNCTION_MAP.get(LabelStep.class.getSimpleName()).apply(null);
        }
        return Arity.MULTI;
    }

    public static void configureMultiLabelProvider() {
        if (STEP_TO_ARITY_FUNCTION_MAP.containsKey(LabelStep.class.getSimpleName())) {
            STEP_TO_ARITY_FUNCTION_MAP.remove(LabelStep.class.getSimpleName());
        }
        STEP_TO_ARITY_FUNCTION_MAP.put(LabelStep.class.getSimpleName(), ((Step) -> Arity.MULTI));
    }

    final private static List<Class> STEP_CLASSES_WITH_OPTIONAL_SINGULAR_ARITY_BEHAVIOR =
            Arrays.asList(HasStep.class,
                    WherePredicateStep.class,
                    AndStep.class,
                    OrStep.class,
                    NotStep.class,
                    IsStep.class,
                    DedupGlobalStep.class,
                    TraversalFilterStep.class,
                    RangeGlobalStep.class,
                    DedupLocalStep.class,
                    ElementStep.class,
                    IndexStep.class,
                    LambdaMapStep.class,
                    LoopsStep.class,
                    MaxLocalStep.class,
                    MeanLocalStep.class,
                    MinLocalStep.class,
                    RangeLocalStep.class,
                    SelectOneStep.class,
                    SelectStep.class,
                    SumLocalStep.class,
                    TailLocalStep.class,
                    WhereTraversalStep.class,
                    TraversalSelectStep.class
            );

    final private static List<Class> STEP_CLASSES_WITH_DEFINITELY_SINGULAR_ARITY_BEHAVIOR =
            Arrays.asList(
                    AddEdgeStep.class,
                    AddVertexStep.class,
                    ConstantStep.class,
                    CountLocalStep.class,
                    EdgeOtherVertexStep.class,
                    ElementMapStep.class,
                    IdStep.class,
                    LabelStep.class,
                    MathStep.class,
                    IdentityStep.class,
                    OrderLocalStep.class,
                    PathStep.class,
                    ProjectStep.class,
                    PropertyKeyStep.class,
                    PropertyMapStep.class,
                    PropertyValueStep.class,
                    SackStep.class,
                    SackValueStep.class,
                    SampleLocalStep.class
            );

    final private static Function<Step, Arity> RETURN_DEFINITELY_SINGLE_ARITY_FOR_STEP_INPUT =
            (step -> Arity.DEFINITELY_SINGULAR);
    final private static Function<Step, Arity> RETURN_MAY_BE_SINGLE_ARITY_FOR_STEP_INPUT =
            (step -> Arity.MAY_BE_SINGULAR);
    final private static Function<Traversal, Arity> RETURN_DEFINITELY_SINGLE_ARITY_FOR_TRAVERSAL_INPUT =
            (step -> Arity.DEFINITELY_SINGULAR);
    final private static Map<String, Function<Step, Arity>> STEP_TO_ARITY_FUNCTION_MAP;
    final private static Map<String, Function<Traversal, Arity>> SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP;

    static {
        STEP_TO_ARITY_FUNCTION_MAP = new HashMap<>();
        for (Class elem : STEP_CLASSES_WITH_DEFINITELY_SINGULAR_ARITY_BEHAVIOR) {
            STEP_TO_ARITY_FUNCTION_MAP.put(elem.getSimpleName(), RETURN_DEFINITELY_SINGLE_ARITY_FOR_STEP_INPUT);
        }

        for (Class elem : STEP_CLASSES_WITH_OPTIONAL_SINGULAR_ARITY_BEHAVIOR) {
            STEP_TO_ARITY_FUNCTION_MAP.put(elem.getSimpleName(), RETURN_MAY_BE_SINGLE_ARITY_FOR_STEP_INPUT);
        }

        STEP_TO_ARITY_FUNCTION_MAP.put(EdgeVertexStep.class.getSimpleName(), StepOutputArityPredictor::wouldEdgeVertexStepHaveSingleResult);
        STEP_TO_ARITY_FUNCTION_MAP.put(TraversalMapStep.class.getSimpleName(), StepOutputArityPredictor::getOutputArityBehaviorForTraversalMapStep);

        SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP = new HashMap<>();
        SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP.put(ValueTraversal.class.getSimpleName(), StepOutputArityPredictor::getOutputArityBehaviorForValueTraversal);
        SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP.put(TokenTraversal.class.getSimpleName(), StepOutputArityPredictor::getOutputArityBehaviorForTokenTraversal);
        SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP.put(IdentityTraversal.class.getSimpleName(), RETURN_DEFINITELY_SINGLE_ARITY_FOR_TRAVERSAL_INPUT);
        SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP.put(ColumnTraversal.class.getSimpleName(), RETURN_DEFINITELY_SINGLE_ARITY_FOR_TRAVERSAL_INPUT);
    }

    private static Arity getStepOutputArity(final Step step, final Arity inputArity) {
        if (step == null) {
            throw new NullPointerException("step is marked non-null but is null");
        }

        final String stepName = step.getClass().getSimpleName();
        final Arity result;
        // Step 1. Check if it is a STEP_TO_ARITY_FUNCTION_MAP member.
        // Step 2. Check if the step is a reducing Barrier Step
        if (STEP_TO_ARITY_FUNCTION_MAP.containsKey(stepName)) {
            result = inputArity.computeArity(STEP_TO_ARITY_FUNCTION_MAP.get(stepName).apply(step));
        } else if (step instanceof ReducingBarrierStep<?, ?>) {
            result = inputArity.computeArity(Arity.DEFINITELY_SINGULAR);
        } else {
            // If the step is not in above list, then consider it a multi arity from here on.
            result = Arity.MULTI;
        }
        return result;
    }


    private static Arity getOutputArity(final Traversal<?, ?> traversal) {
        if (traversal == null) {
            throw new NullPointerException("traversal is marked non-null but is null");
        }

        return getOutputArity(traversal, Arity.DEFINITELY_SINGULAR);
    }

    private static Arity getOutputArity(final Traversal<?, ?> traversal, final Arity inputArity) {
        if (traversal == null) {
            throw new NullPointerException("traversal is marked non-null but is null");
        }

        Arity result = inputArity;
        if (traversal instanceof AbstractLambdaTraversal<?, ?>) {
            if (SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP.containsKey(traversal.getClass().getSimpleName())) {
                result = result.computeArity(SPECIAL_TRAVERSAL_TO_ARITY_FUNCTION_MAP.get(traversal.getClass().getSimpleName())
                        .apply(traversal));
            }
        } else {
            final List<Step> steps = traversal.asAdmin().getSteps();
            for (Step step : steps) {
                result = getStepOutputArity(step, result);
            }
            return result;
        }
        return result;
    }

    public static boolean hasSingularOutputArity(final Traversal<?,?> traversal) {
        if (traversal == null) {
            throw new NullPointerException("traversal is marked non-null but is null");
        }

        final Arity resultArity = getOutputArity(traversal);
        return resultArity.getPriority()==1;
    }

    public static boolean hasAlwaysBoundResult(final Traversal<?,?> traversal) {
        if (traversal == null) {
            throw new NullPointerException("traversal is marked non-null but is null");
        }

        final Arity resultArity = getOutputArity(traversal);
        return resultArity.equals(Arity.DEFINITELY_SINGULAR);
    }

    public static boolean hasMultiOutputArity(final Traversal<?,?> traversal) {
        if (traversal == null) {
            throw new NullPointerException("traversal is marked non-null but is null");
        }

        final Arity resultArity = getOutputArity(traversal);
        return resultArity.getPriority()!=1;
    }

    public static boolean hasSingularOutputArity(final Step<?,?> step) {
        if (step == null) {
            throw new NullPointerException("step is marked non-null but is null");
        }

        final Arity resultArity = getStepOutputArity(step, Arity.DEFINITELY_SINGULAR);
        return resultArity.getPriority()==1;
    }

    public static boolean hasAlwaysBoundResult(final Step<?,?> step) {
        if (step == null) {
            throw new NullPointerException("step is marked non-null but is null");
        }

        final Arity resultArity = getStepOutputArity(step, Arity.DEFINITELY_SINGULAR);
        return resultArity.equals(Arity.DEFINITELY_SINGULAR);
    }

    public static boolean hasMultiOutputArity(final Step<?,?> step) {
        if (step == null) {
            throw new NullPointerException("step is marked non-null but is null");
        }

        final Arity resultArity = getStepOutputArity(step, Arity.DEFINITELY_SINGULAR);
        return resultArity.getPriority()!=1;
    }

}
