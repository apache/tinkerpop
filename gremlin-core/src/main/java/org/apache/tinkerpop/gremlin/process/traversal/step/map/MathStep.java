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
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MathStep<S> extends MapStep<S, Double> implements ByModulating, TraversalParent, Scoping {

    private static final String CURRENT = "_";
    private final String equation;
    private final Set<String> variables;
    private final Expression expression;
    private TraversalRing<Object, Number> traversalRing = new TraversalRing<>();

    public MathStep(final Traversal.Admin traversal, final String equation) {
        super(traversal);
        this.equation = equation;
        this.variables = MathStep.getVariables(this.equation);
        this.expression = new ExpressionBuilder(this.equation)
                .variables(this.variables)
                .build();
    }

    @Override
    protected Double map(final Traverser.Admin<S> traverser) {
        for (final String var : this.variables) {
            this.expression.setVariable(var, TraversalUtil.applyNullable(
                    var.equals(CURRENT) ?
                            traverser.get() :
                            this.getNullableScopeValue(Pop.last, var, traverser),
                    this.traversalRing.next()).doubleValue());
        }
        this.traversalRing.reset();
        return this.expression.evaluate();
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
    public List<Traversal.Admin<Object, Number>> getLocalChildren() {
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
        return this.variables;
    }

    private static final Set<String> getVariables(final String equation) {
        final StringBuilder builder = new StringBuilder();
        final char[] chars = equation.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if ('+' == chars[i] || '-' == chars[i] || '*' == chars[i] ||
                    '/' == chars[i] || '^' == chars[i] || '%' == chars[i] ||
                    '(' == chars[i] || ')' == chars[i])
                builder.append(" ");
            else
                builder.append(chars[i]);
        }
        final Set<String> variables = new LinkedHashSet<>();
        for (final String slot : builder.toString().split(" ")) {
            if (!slot.trim().isEmpty() && !slot.equals("abs") && !slot.equals("acos") &&
                    !slot.equals("asin") && !slot.equals("atan") && !slot.equals("cbrt") &&
                    !slot.equals("ceil") && !slot.equals("cos") && !slot.equals("cosh") &&
                    !slot.equals("exp") && !slot.equals("floor") && !slot.equals("log") &&
                    !slot.equals("log10") && !slot.equals("log2") && !slot.equals("sin") &&
                    !slot.equals("sinh") && !slot.equals("sqrt") && !slot.equals("tan") &&
                    !slot.equals("tanh") && !slot.equals("signum"))
                variables.add(slot);
        }
        return variables;
    }
}
