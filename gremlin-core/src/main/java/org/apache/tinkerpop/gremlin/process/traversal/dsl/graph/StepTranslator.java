/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StepTranslator {
}


        /*{implements Translator<GraphTraversal.Admin<?, ?>> {

    @Override
    public String getSourceLanguage() {
        return "gremlin-java";
    }

    @Override
    public String getTargetLanguage() {
        return "java";
    }

    public GraphTraversal.Admin<?, ?> translate(final GraphTraversal.Admin<?, ?> traversal) {
        this.processStepInstructions(traversal);
        return traversal;
    }

    private final void translateAnonymous(final Object[] arguments) {
        for (final Object object : arguments) {
            if (object instanceof GraphTraversal.Admin) {
                if (((GraphTraversal.Admin) object).getSteps().isEmpty())
                    this.translate((GraphTraversal.Admin) object);

            } else if (object instanceof Object[])
                translateAnonymous((Object[]) object);
        }
    }

    public final Traversal.Admin translateX(final Object traversal) {
        if (traversal instanceof GraphTraversal) {
            if (((GraphTraversal.Admin) traversal).getSteps().isEmpty())
                this.translate((GraphTraversal.Admin) traversal);
        }

        return (Traversal.Admin) traversal;
    }


    private void processStepInstructions(final GraphTraversal.Admin<?, ?> traversal) {
        final List<ByteCode.Instruction> instructions = traversal.getByteCode().getStepInstructions();
        for (int i = 0; i < instructions.size(); i++) {

            final ByteCode.Instruction instruction = instructions.get(i);
            final Object[] arguments = instruction.getArguments();
            translateAnonymous(arguments);

            switch (instruction.getOperator()) {
                case GraphTraversal.Symbols.map:
                    traversal.addStep(arguments[0] instanceof Traversal ?
                            new TraversalMapStep<>(traversal, (Traversal) arguments[0]) :
                            new LambdaMapStep<>(traversal, (Function) arguments[0]));
                    break;
                case GraphTraversal.Symbols.flatMap:
                    traversal.addStep(arguments[0] instanceof Traversal ?
                            new TraversalFlatMapStep<>(traversal, (Traversal) arguments[0]) :
                            new LambdaFlatMapStep<>(traversal, (Function) arguments[0]));
                    break;
                case GraphTraversal.Symbols.id:
                    traversal.addStep(new IdStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.label:
                    traversal.addStep(new LabelStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.identity:
                    traversal.addStep(new IdentityStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.constant:
                    traversal.addStep(new ConstantStep<>(traversal, arguments[0]));
                    break;
                case GraphTraversal.Symbols.E:
                    traversal.addStep(new GraphStep<>(traversal, Edge.class, true, arguments));
                    break;
                case GraphTraversal.Symbols.V:
                    traversal.addStep(new GraphStep<>(traversal, Vertex.class, traversal.getSteps().isEmpty(), arguments));
                    break;
                case GraphTraversal.Symbols.to:
                    if (1 == arguments.length)
                        ((AddEdgeStep) traversal.getEndStep()).addTo(arguments[0] instanceof Vertex ?
                                (Vertex) arguments[0] :
                                translateX(__.select((String) arguments[0])));
                    else
                        traversal.addStep(new VertexStep<>(traversal, Vertex.class, (Direction) arguments[0], (String[]) arguments[1]));
                    break;
                case GraphTraversal.Symbols.out:
                    traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.OUT, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.in:
                    traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.IN, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.both:
                    traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.BOTH, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.toE:
                    traversal.addStep(new VertexStep<>(traversal, Edge.class, (Direction) arguments[0], (String[]) arguments[1]));
                    break;
                case GraphTraversal.Symbols.outE:
                    traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.OUT, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.inE:
                    traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.IN, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.bothE:
                    traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.BOTH, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.toV:
                    traversal.addStep(new EdgeVertexStep(traversal, (Direction) arguments[0]));
                    break;
                case GraphTraversal.Symbols.outV:
                    traversal.addStep(new EdgeVertexStep(traversal, Direction.OUT));
                    break;
                case GraphTraversal.Symbols.inV:
                    traversal.addStep(new EdgeVertexStep(traversal, Direction.IN));
                    break;
                case GraphTraversal.Symbols.bothV:
                    traversal.addStep(new EdgeVertexStep(traversal, Direction.BOTH));
                    break;
                case GraphTraversal.Symbols.otherV:
                    traversal.addStep(new EdgeOtherVertexStep(traversal));
                    break;
                case GraphTraversal.Symbols.order:
                    traversal.addStep(arguments.length == 0 || Scope.global == arguments[0] ?
                            new OrderGlobalStep<>(traversal) :
                            new OrderLocalStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.properties:
                    traversal.addStep(new PropertiesStep<>(traversal, PropertyType.PROPERTY, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.values:
                    traversal.addStep(new PropertiesStep<>(traversal, PropertyType.VALUE, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.propertyMap:
                    traversal.addStep(0 != arguments.length && arguments[0] instanceof Boolean ?
                            new PropertyMapStep<>(traversal, (boolean) arguments[0], PropertyType.PROPERTY, (String[]) arguments[1]) :
                            new PropertyMapStep<>(traversal, false, PropertyType.PROPERTY, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.valueMap:
                    traversal.addStep(0 != arguments.length && arguments[0] instanceof Boolean ?
                            new PropertyMapStep<>(traversal, (boolean) arguments[0], PropertyType.VALUE, (String[]) arguments[1]) :
                            new PropertyMapStep<>(traversal, false, PropertyType.VALUE, (String[]) arguments));
                    break;
                case GraphTraversal.Symbols.select:
                    if (arguments[0] instanceof Column)
                        traversal.addStep(new TraversalMapStep<>(traversal, new ColumnTraversal((Column) arguments[0])));
                    else if (arguments[0] instanceof Pop) {
                        if (arguments[1] instanceof String)
                            traversal.addStep(new SelectOneStep<>(traversal, (Pop) arguments[0], (String) arguments[1]));
                        else {
                            final String[] selectKeys = new String[((String[]) arguments[3]).length + 2];
                            selectKeys[0] = (String) arguments[1];
                            selectKeys[1] = (String) arguments[2];
                            System.arraycopy(arguments[3], 0, selectKeys, 2, ((String[]) arguments[3]).length);
                            traversal.addStep(new SelectStep<>(traversal, (Pop) arguments[0], selectKeys));
                        }
                    } else {
                        if (1 == arguments.length && arguments[0] instanceof String)
                            traversal.addStep(new SelectOneStep<>(traversal, null, (String) arguments[0]));
                        else {
                            final String[] selectKeys = new String[((String[]) arguments[2]).length + 2];
                            selectKeys[0] = (String) arguments[0];
                            selectKeys[1] = (String) arguments[1];
                            System.arraycopy(arguments[2], 0, selectKeys, 2, ((String[]) arguments[2]).length);
                            traversal.addStep(new SelectStep<>(traversal, null, selectKeys));
                        }
                    }
                    break;
                case GraphTraversal.Symbols.key:
                    traversal.addStep(new PropertyKeyStep(traversal));
                    break;
                case GraphTraversal.Symbols.value:
                    traversal.addStep(new PropertyValueStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.path:
                    traversal.addStep(new PathStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.match:
                    traversal.addStep(new MatchStep<>(traversal, ConnectiveStep.Connective.AND, (Traversal[]) arguments));
                    break;
                case GraphTraversal.Symbols.sack:
                    if (0 == arguments.length)
                        traversal.addStep(new SackStep<>(traversal));
                    else {
                        final SackValueStep<?, ?, ?> sackValueStep = new SackValueStep<>(traversal, (BiFunction) arguments[0]);
                        if (2 == arguments.length)
                            sackValueStep.modulateBy((String) arguments[1]);
                        traversal.addStep(sackValueStep);
                    }
                    break;
                case GraphTraversal.Symbols.loops:
                    traversal.addStep(new LoopsStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.project:
                    final String[] projectKeys = new String[((String[]) arguments[1]).length + 1];
                    projectKeys[0] = (String) arguments[0];
                    System.arraycopy((arguments[1]), 0, projectKeys, 1, ((String[]) arguments[1]).length);
                    traversal.addStep(new ProjectStep<>(traversal, projectKeys));
                    break;
                case GraphTraversal.Symbols.unfold:
                    traversal.addStep(new UnfoldStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.fold:
                    traversal.addStep(0 == arguments.length ?
                            new FoldStep<>(traversal) :
                            new FoldStep<>(traversal, arguments[0] instanceof Supplier ?
                                    (Supplier) arguments[0] :
                                    new ConstantSupplier(arguments[0]), (BiFunction) arguments[1]));
                    break;
                case GraphTraversal.Symbols.count:
                    traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                            new CountGlobalStep<>(traversal) :
                            new CountLocalStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.sum:
                    traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                            new SumGlobalStep<>(traversal) :
                            new SumLocalStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.max:
                    traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                            new MaxGlobalStep<>(traversal) :
                            new MaxLocalStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.min:
                    traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                            new MinGlobalStep<>(traversal) :
                            new MinLocalStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.mean:
                    traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                            new MeanGlobalStep<>(traversal) :
                            new MeanLocalStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.group:
                    traversal.addStep(0 == arguments.length ?
                            new GroupStep<>(traversal) :
                            new GroupSideEffectStep<>(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.groupV3d0:
                    traversal.addStep(0 == arguments.length ?
                            new GroupStepV3d0<>(traversal) :
                            new GroupSideEffectStepV3d0<>(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.groupCount:
                    traversal.addStep(0 == arguments.length ?
                            new GroupCountStep<>(traversal) :
                            new GroupCountSideEffectStep<>(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.tree:
                    traversal.addStep(0 == arguments.length ?
                            new TreeStep<>(traversal) :
                            new TreeSideEffectStep<>(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.addV:
                    if (traversal.getSteps().isEmpty())
                        traversal.addStep(new AddVertexStartStep(traversal, 0 == arguments.length ? null : (String) arguments[0]));
                    else {
                        traversal.addStep(1 == arguments.length ?
                                new AddVertexStep<>(traversal, (String) arguments[0]) :
                                new AddVertexStep<>(traversal, null));
                    }
                    if (arguments.length > 1) {
                        for (int j = 0; j < arguments.length; j = j + 2) {
                            traversal.addStep(new AddPropertyStep<>(traversal, null, arguments[j], arguments[j + 1]));
                        }
                    }
                    break;
                case GraphTraversal.Symbols.addE:
                    if (1 == arguments.length)
                        traversal.addStep(new AddEdgeStep<>(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.from:
                    ((AddEdgeStep) traversal.getEndStep()).addFrom(arguments[0] instanceof Vertex ?
                            (Vertex) arguments[0] :
                            translateX(__.select((String) arguments[0])));
                    break;
                case GraphTraversal.Symbols.filter:
                    traversal.addStep(arguments[0] instanceof Traversal ?
                            new TraversalFilterStep<>(traversal, (Traversal) arguments[0]) :
                            new LambdaFilterStep<>(traversal, (Predicate) arguments[0]));
                    break;
                case GraphTraversal.Symbols.or:
                    traversal.addStep(new OrStep<>(traversal, (Traversal[]) arguments));
                    break;
                case GraphTraversal.Symbols.and:
                    traversal.addStep(new AndStep<>(traversal, (Traversal[]) arguments));
                    break;
                case GraphTraversal.Symbols.inject:
                    traversal.addStep(new InjectStep<>(traversal, arguments));
                    break;
                case GraphTraversal.Symbols.dedup:
                    if (0 == arguments.length)
                        traversal.addStep(new DedupGlobalStep<>(traversal));
                    else {
                        traversal.addStep(arguments instanceof String[] ?
                                new DedupGlobalStep<>(traversal, (String[]) arguments) :
                                arguments[0] == Scope.global ?
                                        new DedupGlobalStep<>(traversal, (String[]) arguments[1]) :
                                        new DedupLocalStep<>(traversal));
                    }
                    break;
                case GraphTraversal.Symbols.where:
                    traversal.addStep(arguments[0] instanceof Traversal.Admin ?
                            TraversalHelper.getVariableLocations((Traversal.Admin) arguments[0]).isEmpty() ?
                                    new TraversalFilterStep<>(traversal, (Traversal.Admin) arguments[0]) :
                                    new WhereTraversalStep<>(traversal, (Traversal.Admin) arguments[0]) :
                            arguments[0] instanceof String ?
                                    new WherePredicateStep<>(traversal, Optional.of((String) arguments[0]), (P) arguments[1]) :
                                    new WherePredicateStep<>(traversal, Optional.empty(), (P) arguments[0]));
                    break;
                case GraphTraversal.Symbols.has:
                    if (1 == arguments.length) {
                        traversal.addStep(new TraversalFilterStep<>(traversal, translateX(__.values((String) arguments[0]))));
                    } else if (2 == arguments.length) {
                        final String propertyKey = arguments[0] instanceof T ? ((T) arguments[0]).getAccessor() : (String) arguments[0];
                        if (arguments[1] instanceof Traversal.Admin) {
                            traversal.addStep(
                                    new TraversalFilterStep<>(traversal, ((Traversal.Admin) arguments[1]).addStep(0,
                                            new PropertiesStep((Traversal.Admin) arguments[1], PropertyType.VALUE, propertyKey))));
                        } else {
                            final P predicate = arguments[1] instanceof P ? (P) arguments[1] : P.eq(arguments[1]);
                            traversal.addStep(new HasStep<>(traversal, HasContainer.makeHasContainers(propertyKey, predicate)));
                        }
                    } else if (3 == arguments.length) {
                        traversal.addStep(new HasStep<>(traversal, HasContainer.makeHasContainers(T.label.getAccessor(), P.eq(arguments[0]))));
                        traversal.addStep(new HasStep<>(traversal, HasContainer.makeHasContainers(arguments[1] instanceof T ?
                                        ((T) arguments[1]).getAccessor() :
                                        (String) arguments[1],
                                arguments[2] instanceof P ?
                                        (P) arguments[2] :
                                        P.eq(arguments[2]))));
                    }
                    break;
                case GraphTraversal.Symbols.hasNot:
                    traversal.addStep(new NotStep<>(traversal, translateX(__.values((String) arguments[0]))));
                    break;
                case GraphTraversal.Symbols.hasLabel:
                    traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                            HasContainer.makeHasContainers(T.label.getAccessor(), P.eq(arguments[0])) :
                            HasContainer.makeHasContainers(T.label.getAccessor(), P.within(arguments))));
                    break;
                case GraphTraversal.Symbols.hasId:
                    traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                            HasContainer.makeHasContainers(T.id.getAccessor(), P.eq(arguments[0])) :
                            HasContainer.makeHasContainers(T.id.getAccessor(), P.within(arguments))));
                    break;
                case GraphTraversal.Symbols.hasKey:
                    traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                            HasContainer.makeHasContainers(T.key.getAccessor(), P.eq(arguments[0])) :
                            HasContainer.makeHasContainers(T.key.getAccessor(), P.within(arguments))));
                    break;
                case GraphTraversal.Symbols.hasValue:
                    traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                            HasContainer.makeHasContainers(T.value.getAccessor(), P.eq(arguments[0])) :
                            HasContainer.makeHasContainers(T.value.getAccessor(), P.within(arguments))));
                    break;
                case GraphTraversal.Symbols.is:
                    traversal.addStep(new IsStep<>(traversal, arguments[0] instanceof P ?
                            (P) arguments[0] :
                            P.eq(arguments[0])));
                    break;
                case GraphTraversal.Symbols.not:
                    traversal.addStep(new NotStep<>(traversal, (Traversal) arguments[0]));
                    break;
                case GraphTraversal.Symbols.range:
                    if (2 == arguments.length)
                        traversal.addStep(new RangeGlobalStep<>(traversal, (long) arguments[0], (long) arguments[1]));
                    else
                        traversal.addStep(Scope.global == arguments[0] ?
                                new RangeGlobalStep<>(traversal, (long) arguments[1], (long) arguments[2]) :
                                new RangeLocalStep<>(traversal, (long) arguments[1], (long) arguments[2]));
                    break;
                case GraphTraversal.Symbols.limit:
                    if (1 == arguments.length)
                        traversal.addStep(new RangeGlobalStep<>(traversal, 0, (long) arguments[0]));
                    else
                        traversal.addStep(Scope.global == arguments[0] ?
                                new RangeGlobalStep<>(traversal, 0, (long) arguments[1]) :
                                new RangeLocalStep<>(traversal, 0, (long) arguments[1]));
                    break;
                case GraphTraversal.Symbols.tail:
                    if (0 == arguments.length)
                        traversal.addStep(new TailGlobalStep<>(traversal, 1L));
                    else if (arguments[0] instanceof Long)
                        traversal.addStep(new TailGlobalStep<>(traversal, (long) arguments[0]));
                    else if (1 == arguments.length)
                        traversal.addStep(Scope.global == arguments[0] ?
                                new TailGlobalStep<>(traversal, 1L) :
                                new TailLocalStep<>(traversal, 1L));
                    else
                        traversal.addStep(Scope.global == arguments[0] ?
                                new TailGlobalStep<>(traversal, (long) arguments[1]) :
                                new TailLocalStep<>(traversal, (long) arguments[1]));
                    break;
                case GraphTraversal.Symbols.coin:
                    traversal.addStep(new CoinStep<>(traversal, (double) arguments[0]));
                    break;
                case GraphTraversal.Symbols.timeLimit:
                    traversal.addStep(new TimeLimitStep<>(traversal, (long) arguments[0]));
                    break;
                case GraphTraversal.Symbols.simplePath:
                    traversal.addStep(new SimplePathStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.cyclicPath:
                    traversal.addStep(new CyclicPathStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.sample:
                    if (1 == arguments.length)
                        traversal.addStep(new SampleGlobalStep<>(traversal, (int) arguments[0]));
                    else
                        traversal.addStep(Scope.global == arguments[0] ?
                                new SampleGlobalStep<>(traversal, (int) arguments[1]) :
                                new SampleLocalStep<>(traversal, (int) arguments[1]));
                    break;
                case GraphTraversal.Symbols.drop:
                    traversal.addStep(new DropStep<>(traversal));
                    break;
                case GraphTraversal.Symbols.sideEffect:
                    traversal.addStep(arguments[0] instanceof Traversal ?
                            new TraversalSideEffectStep<>(traversal, (Traversal) arguments[0]) :
                            new LambdaSideEffectStep<>(traversal, (Consumer) arguments[0]));
                    break;
                case GraphTraversal.Symbols.property:
                    final VertexProperty.Cardinality cardinality = arguments[0] instanceof VertexProperty.Cardinality ? (VertexProperty.Cardinality) arguments[0] : null;
                    final Object key = null == cardinality ? arguments[0] : arguments[1];
                    final Object value = null == cardinality ? arguments[1] : arguments[2];
                    final Object[] keyValues = null == cardinality ? (Object[]) arguments[2] : (Object[]) arguments[3];
                    if ((traversal.getEndStep() instanceof AddVertexStep || traversal.getEndStep() instanceof AddEdgeStep
                            || traversal.getEndStep() instanceof AddVertexStartStep) && keyValues.length == 0 && null == cardinality) {
                        ((Mutating) traversal.getEndStep()).addPropertyMutations(key, value);
                    } else {
                        traversal.addStep(new AddPropertyStep(traversal, cardinality, key, value));
                        ((AddPropertyStep) traversal.getEndStep()).addPropertyMutations(keyValues);
                    }
                    break;
                case GraphTraversal.Symbols.cap:
                    traversal.addStep(1 == arguments.length ?
                            new SideEffectCapStep<>(traversal, (String) arguments[0]) :
                            new SideEffectCapStep<>(traversal, (String) arguments[0], (String[]) arguments[1]));
                    break;
                case GraphTraversal.Symbols.aggregate:
                    traversal.addStep(new AggregateStep<>(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.store:
                    traversal.addStep(new StoreStep<>(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.subgraph:
                    traversal.addStep(new SubgraphStep(traversal, (String) arguments[0]));
                    break;
                case GraphTraversal.Symbols.profile:
                    traversal.addStep(new ProfileSideEffectStep<>(traversal, 0 == arguments.length ?
                            ProfileSideEffectStep.DEFAULT_METRICS_KEY :
                            (String) arguments[0]));
                    if (0 == arguments.length)
                        traversal.addStep(new SideEffectCapStep<>(traversal, ProfileSideEffectStep.DEFAULT_METRICS_KEY));
                    break;
                case GraphTraversal.Symbols.branch:
                    final BranchStep branchStep = new BranchStep<>(traversal);
                    branchStep.setBranchTraversal(arguments[0] instanceof Traversal.Admin ?
                            (Traversal.Admin) arguments[0] :
                            translateX(__.map((Function) arguments[0]).asAdmin()));
                    traversal.addStep(branchStep);
                    break;
                case GraphTraversal.Symbols.choose:
                    if (1 == arguments.length)
                        traversal.addStep(new ChooseStep<>(traversal, arguments[0] instanceof Traversal.Admin ?
                                (Traversal.Admin) arguments[0] :
                                translateX(__.map(new FunctionTraverser<>((Function) arguments[0])).asAdmin())));
                    else
                        traversal.addStep(new ChooseStep<>(traversal,
                                arguments[0] instanceof Traversal.Admin ?
                                        (Traversal.Admin) arguments[0] :
                                        translateX(__.filter(new PredicateTraverser<>((Predicate) arguments[0])).asAdmin()),
                                (Traversal.Admin) arguments[1], (Traversal.Admin) arguments[2]));
                    break;
                case GraphTraversal.Symbols.optional:
                    traversal.addStep(new ChooseStep<>(traversal, (Traversal.Admin) arguments[0], ((Traversal.Admin) arguments[0]).clone(), translateX(__.identity().asAdmin())));
                    break;
                case GraphTraversal.Symbols.union:
                    traversal.addStep(new UnionStep<>(traversal, Arrays.copyOf(arguments, arguments.length, Traversal.Admin[].class)));
                    break;
                case GraphTraversal.Symbols.coalesce:
                    traversal.addStep(new CoalesceStep<>(traversal, Arrays.copyOf(arguments, arguments.length, Traversal.Admin[].class)));
                    break;
                case GraphTraversal.Symbols.repeat:
                    RepeatStep.addRepeatToTraversal(traversal, (Traversal.Admin) arguments[0]);
                    break;
                case GraphTraversal.Symbols.emit:
                    if (0 == arguments.length)
                        RepeatStep.addEmitToTraversal(traversal, TrueTraversal.instance());
                    else if (arguments[0] instanceof Traversal.Admin)
                        RepeatStep.addEmitToTraversal(traversal, (Traversal.Admin) arguments[0]);
                    else
                        RepeatStep.addEmitToTraversal(traversal, translateX(__.filter((Predicate) arguments[0]).asAdmin()));
                    break;
                case GraphTraversal.Symbols.until:
                    if (arguments[0] instanceof Traversal.Admin)
                        RepeatStep.addUntilToTraversal(traversal, (Traversal.Admin) arguments[0]);
                    else
                        RepeatStep.addUntilToTraversal(traversal, translateX(__.filter((Predicate) arguments[0]).asAdmin()));
                    break;
                case GraphTraversal.Symbols.times:
                    if (traversal.getEndStep() instanceof TimesModulating)
                        ((TimesModulating) traversal.getEndStep()).modulateTimes((int) arguments[0]);
                    else
                        RepeatStep.addUntilToTraversal(traversal, new LoopTraversal<>((int) arguments[0]));
                    break;
                case GraphTraversal.Symbols.barrier:
                    traversal.addStep(0 == arguments.length ?
                            new NoOpBarrierStep<>(traversal) :
                            arguments[0] instanceof Consumer ?
                                    new LambdaCollectingBarrierStep<>(traversal, (Consumer) arguments[0], Integer.MAX_VALUE) :
                                    new NoOpBarrierStep<>(traversal, (int) arguments[0]));
                    break;
                case GraphTraversal.Symbols.local:
                    traversal.addStep(new LocalStep<>(traversal, (Traversal.Admin) arguments[0]));
                    break;
                case GraphTraversal.Symbols.pageRank:
                    traversal.addStep(new PageRankVertexProgramStep(traversal, 0 == arguments.length ? 0.85d : (double) arguments[0]));
                    break;
                case GraphTraversal.Symbols.peerPressure:
                    traversal.addStep(new PeerPressureVertexProgramStep(traversal));
                    break;
                case GraphTraversal.Symbols.program:
                    traversal.addStep(new ProgramVertexProgramStep(traversal, (VertexProgram) arguments[0]));
                    break;
                case GraphTraversal.Symbols.by:
                    if (0 == arguments.length)
                        ((ByModulating) traversal.getEndStep()).modulateBy();
                    else if (1 == arguments.length) {
                        if (arguments[0] instanceof String)
                            ((ByModulating) traversal.getEndStep()).modulateBy((String) arguments[0]);
                        else if (arguments[0] instanceof T)
                            ((ByModulating) traversal.getEndStep()).modulateBy((T) arguments[0]);
                        else if (arguments[0] instanceof Traversal.Admin)
                            ((ByModulating) traversal.getEndStep()).modulateBy((Traversal.Admin) arguments[0]);
                        else if (arguments[0] instanceof Function)
                            ((ByModulating) traversal.getEndStep()).modulateBy((Function) arguments[0]);
                        else if (arguments[0] instanceof Order)
                            ((ByModulating) traversal.getEndStep()).modulateBy((Order) arguments[0]);
                        else if (arguments[0] instanceof Comparator)
                            ((ByModulating) traversal.getEndStep()).modulateBy((Comparator) arguments[0]);
                    } else {
                        if (arguments[0] instanceof String)
                            ((ByModulating) traversal.getEndStep()).modulateBy((String) arguments[0], (Comparator) arguments[1]);
                        else if (arguments[0] instanceof T)
                            ((ByModulating) traversal.getEndStep()).modulateBy((T) arguments[0], (Comparator) arguments[1]);
                        else if (arguments[0] instanceof Traversal.Admin)
                            ((ByModulating) traversal.getEndStep()).modulateBy((Traversal.Admin) arguments[0], (Comparator) arguments[1]);
                        else
                            ((ByModulating) traversal.getEndStep()).modulateBy((Function) arguments[0], (Comparator) arguments[1]);
                    }
                    break;
                case GraphTraversal.Symbols.as:
                    if (traversal.getSteps().isEmpty())
                        traversal.addStep(new StartStep<>(traversal));
                    traversal.getEndStep().addLabel((String) arguments[0]);
                    if (arguments.length > 1) {
                        final String[] otherLabels = (String[]) arguments[1];
                        for (int j = 0; j < otherLabels.length; j++) {
                            traversal.getEndStep().addLabel(otherLabels[j]);
                        }
                    }
                    break;
                case GraphTraversal.Symbols.option:
                    if (1 == arguments.length)
                        ((TraversalOptionParent) traversal.getEndStep()).addGlobalChildOption(TraversalOptionParent.Pick.any, (Traversal.Admin) arguments[0]);
                    else
                        ((TraversalOptionParent) traversal.getEndStep()).addGlobalChildOption(arguments[0], (Traversal.Admin) arguments[1]);
                    break;
                default:
                    throw new IllegalArgumentException("The provided step name is not supported by " + StepTranslator.class.getSimpleName() + ": " + instruction.getOperator());
            }
        }
    }

     public GraphTraversalSource x(final GraphTraversalSource traversalSource, final String sourceName, final Object... arguments) {
        final GraphTraversalSource clone = traversalSource.clone();
        switch (sourceName) {
            case TraversalSourceSymbols.withComputer:
                Class<? extends GraphComputer> graphComputerClass;
                try {
                    graphComputerClass = ((Computer) arguments[0]).apply(clone.getGraph()).getClass();
                } catch (final Exception e) {
                    graphComputerClass = ((Computer) arguments[0]).getGraphComputerClass();
                }
                final List<TraversalStrategy<?>> graphComputerStrategies = TraversalStrategies.GlobalCache.getStrategies(graphComputerClass).toList();
                final TraversalStrategy[] traversalStrategies = new TraversalStrategy[graphComputerStrategies.size() + 1];
                traversalStrategies[0] = new VertexProgramStrategy(((Computer) arguments[0]));
                for (int i = 0; i < graphComputerStrategies.size(); i++) {
                    traversalStrategies[i + 1] = graphComputerStrategies.get(i);
                }
                clone.getStrategies().addStrategies(traversalStrategies);
                return clone;
            case Symbols.withPath:
                RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.PATH);
                return clone;
            case Symbols.withBulk:
                if (1 == arguments.length && !((boolean) arguments[0]))
                    RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.ONE_BULK);
                return clone;
            case TraversalSourceSymbols.withSideEffect:
                SideEffectStrategy.addSideEffect(clone.getStrategies(), (String) arguments[0],
                        arguments[1] instanceof Supplier ? (Supplier) arguments[1] : new ConstantSupplier<>(arguments[1]), 2 == arguments.length ? null : (BinaryOperator) arguments[2]);
                return clone;
            case TraversalSourceSymbols.withSack:
                if (1 == arguments.length) {
                    clone.getStrategies().addStrategies(SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                            (Supplier) arguments[0] :
                            new ConstantSupplier<>(arguments[0])).create());
                } else if (2 == arguments.length) {
                    if (arguments[1] instanceof UnaryOperator)
                        clone.getStrategies().addStrategies(SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                                (Supplier) arguments[0] :
                                new ConstantSupplier<>(arguments[0])).splitOperator((UnaryOperator) arguments[1]).create());
                    else
                        clone.getStrategies().addStrategies((SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                                (Supplier) arguments[0] :
                                new ConstantSupplier<>(arguments[0])).mergeOperator((BinaryOperator) arguments[1]).create()));
                } else {
                    clone.getStrategies().addStrategies(SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                            (Supplier) arguments[0] :
                            new ConstantSupplier<>(arguments[0])).splitOperator((UnaryOperator) arguments[1]).mergeOperator((BinaryOperator) arguments[2]).create());
                }
                return clone;
            default:
                throw new IllegalArgumentException("The provided step name is not supported by " + StepTranslator.class.getSimpleName() + ": " + sourceName);
        }

    }
}*/
