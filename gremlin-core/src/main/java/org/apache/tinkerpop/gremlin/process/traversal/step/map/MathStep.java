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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MathStep<S> extends MapStep<S, Double> implements ByModulating, TraversalParent, Scoping, PathProcessor {

    private static final String CURRENT = "_";
    private final String equation;
    private final TinkerExpression expression;
    private TraversalRing<S, Number> traversalRing = new TraversalRing<>();
    private Set<String> keepLabels;

    public MathStep(final Traversal.Admin traversal, final String equation) {
        super(traversal);
        this.equation = equation;
        this.expression = new TinkerExpression(equation, MathStep.getVariables(this.equation));
    }

    @Override
    protected Traverser.Admin<Double> processNextStart() {
        final Traverser.Admin traverser = this.starts.next();

        final Expression localExpression = new Expression(this.expression.getExpression());
        boolean productive = true;
        for (final String var : this.expression.getVariables()) {
            final TraversalProduct product = var.equals(CURRENT) ?
                    TraversalUtil.produce(traverser, this.traversalRing.next()) :
                    TraversalUtil.produce((S) this.getNullableScopeValue(Pop.last, var, traverser), this.traversalRing.next());

            if (!product.isProductive()) {
                productive = false;
                break;
            }

            final Object o = product.get();

            // it's possible for ElementValueTraversal to return null or something that is possibly not a Number.
            // worth a check to try to return a nice error message. The TraversalRing<S, Number> is a bit optimistic
            // given type erasure. It could easily end up otherwise.
            if (!(o instanceof Number))
                throw new IllegalStateException(String.format(
                        "The variable %s for math() step must resolve to a Number - it is instead of type %s with value %s",
                        var, Objects.isNull(o) ? "null" : o.getClass().getName(), o));

            localExpression.setVariable(var, ((Number) o).doubleValue());
        }
        this.traversalRing.reset();

        // if at least one of the traversals wasnt productive it will filter
        return productive ?
                PathProcessor.processTraverserPathLabels(traverser.split(localExpression.evaluate(), this), this.keepLabels) :
                EmptyTraverser.instance();
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    @Override
    public ElementRequirement getMaxRequirement() {
        // this is a trick i saw in DedupGlobalStep that allows ComputerVerificationStrategy to be happy for OLAP.
        // it's a bit more of a hack here. in DedupGlobalStep, the dedup operation really only just needs the ID, but
        // here the true max requirement is PROPERTIES, but because of how map() works in this implementation in
        // relation to CURRENT, if we don't access the path labels, then we really only just operate on the stargraph
        // and are thus OLAP safe. In tracing around the code a bit, I don't see a problem with taking this approach,
        // but I suppose a better way might be make it more clear when this step is dealing with an actual path and
        // when it is not and/or adjust ComputerVerificationStrategy to cope with the situation where math() is only
        // dealing with the local stargraph.
        return (this.expression.getVariables().contains(CURRENT) && this.expression.getVariables().size() == 1) ?
                ElementRequirement.ID : PathProcessor.super.getMaxRequirement();
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
        if (this.expression.getVariables().contains(CURRENT)) {
            final Set<String> temp = new HashSet<>(this.expression.getVariables());
            temp.remove(CURRENT);
            return temp;
        } else
            return this.expression.getVariables();
    }

    @Override
    public Set<ScopingInfo> getScopingInfo() {
        final Set<String> labels = this.getScopeKeys();
        final Set<ScopingInfo> scopingInfoSet = new HashSet<>();
        for (String label : labels) {
            final ScopingInfo scopingInfo = new ScopingInfo();
            scopingInfo.label = label;
            scopingInfo.pop = Pop.last;
            scopingInfoSet.add(scopingInfo);
        }
        return scopingInfoSet;
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

    private static final Pattern VARIABLE_PATTERN =  Pattern.compile("\\b(?![0-9]+|\\b(?:" +
            String.join("|", FUNCTIONS) + ")\\b)([a-zA-Z_][a-zA-Z0-9_]*)\\b");

    protected static final Set<String> getVariables(final String equation) {
        final Matcher matcher = VARIABLE_PATTERN.matcher(equation);
        final Set<String> variables = new LinkedHashSet<>();
        while (matcher.find()) {
            variables.add(matcher.group());
        }
        return variables;
    }

    /**
     * A wrapper for the {@code Expression} class. That class is not marked {@code Serializable} and therefore gives
     * problems in OLAP specifically with Spark. This wrapper allows the {@code Expression} to be serialized in that
     * context with Java serialization.
     */
    public static class TinkerExpression implements Serializable {
        private transient Expression expression;
        private final String equation;
        private final Set<String> variables;

        public TinkerExpression(final String equation, final Set<String> variables) {
            this.variables = variables;
            this.equation = equation;
        }

        public Expression getExpression() {
            if (null == expression) {
                this.expression = new ExpressionBuilder(this.equation)
                        .variables(this.variables)
                        .implicitMultiplication(false)
                        .build();
            }
            return expression;
        }

        public Set<String> getVariables() {
            return variables;
        }
    }

}
