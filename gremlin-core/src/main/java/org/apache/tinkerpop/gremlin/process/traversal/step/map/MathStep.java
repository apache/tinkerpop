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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MathStep<S> extends MapStep<S, Double> implements ByModulating, TraversalParent, Scoping, PathProcessor {

    private static final String CURRENT = "_";
    private final String equation;
    private final Set<String> variables;
    private final Expression expression;
    private TraversalRing<S, Number> traversalRing = new TraversalRing<>();
    private Set<String> keepLabels;

    public MathStep(final Traversal.Admin traversal, final String equation) {
        super(traversal);
        this.equation = equation;
        this.variables = MathStep.getVariables(this.equation);
        this.expression = new ExpressionBuilder(this.equation)
                .variables(this.variables)
                .implicitMultiplication(false)
                .build();
    }

    @Override
    protected Traverser.Admin<Double> processNextStart() {
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }

    @Override
    protected Double map(final Traverser.Admin<S> traverser) {
        final Expression localExpression = new Expression(this.expression);
        for (final String var : this.variables) {
            localExpression.setVariable(var,
                    var.equals(CURRENT) ?
                            TraversalUtil.applyNullable(traverser, this.traversalRing.next()).doubleValue() :
                            TraversalUtil.applyNullable((S) this.getNullableScopeValue(Pop.last, var, traverser), this.traversalRing.next()).doubleValue());
        }
        this.traversalRing.reset();
        return localExpression.evaluate();
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.equation, this.traversalRing);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.equation.hashCode() ^ this.traversalRing.hashCode();
    }

    @Override
    public List<Traversal.Admin<S, Number>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public MathStep<S> clone() {
        final MathStep<S> clone = (MathStep<S>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public Set<String> getScopeKeys() {
        if (this.variables.contains(CURRENT)) {
            final Set<String> temp = new HashSet<>(this.variables);
            temp.remove(CURRENT);
            return temp;
        } else
            return this.variables;
    }

    @Override
    public void setKeepLabels(final Set<String> labels) {
        this.keepLabels = labels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    ///

    private static final String[] FUNCTIONS = new String[]{
            "abs", "acos", "asin", "atan",
            "cbrt", "ceil", "cos", "cosh",
            "exp",
            "floor",
            "log", "log10", "log2",
            "signum", "sin", "sinh", "sqrt",
            "tan", "tanh"
    };

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\b(?!" +
            String.join("|", FUNCTIONS) + "|([0-9]+))([a-zA-Z_][a-zA-Z0-9_]*)\\b");

    protected static final Set<String> getVariables(final String equation) {
        final Matcher matcher = VARIABLE_PATTERN.matcher(equation);
        final Set<String> variables = new LinkedHashSet<>();
        while (matcher.find()) {
            variables.add(matcher.group());
        }
        return variables;
    }

}
