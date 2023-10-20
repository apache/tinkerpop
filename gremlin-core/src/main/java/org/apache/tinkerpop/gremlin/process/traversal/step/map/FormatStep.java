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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reference implementation for String format step, a mid-traversal step which will handle result formatting
 * to string values. If the incoming traverser is a non-String value then an {@code IllegalArgumentException}
 * will be thrown.
 *
 * @author Valentyn Kahamlyk
 */
public final class FormatStep<S> extends MapStep<S, String> implements TraversalParent, Scoping, PathProcessor {

    private String format;
    private Set<String> variables;
    private TraversalRing<S, String> traversalRing = new TraversalRing<>();
    private Set<String> keepLabels;
    private final Map<String, String> values = new HashMap<>();
    private Matcher matcher;

    public FormatStep(final Traversal.Admin traversal, final String format) {
        super(traversal);
        this.format = format;
        this.matcher = VARIABLE_PATTERN.matcher(format);
        this.variables = getVariables();
    }

    @Override
    protected Traverser.Admin<String> processNextStart() {
        values.clear();

        final Traverser.Admin traverser = this.starts.next();

        boolean productive = true;
        for (final String var : variables) {
            final Object current = traverser.get();
            // try to get property value
            if (current instanceof Element) {
                final Property prop = ((Element) current).property(var);
                if (prop != null && prop.isPresent()) {
                    addValue(var, prop.value());
                    continue;
                }
            } else if (current instanceof Map) {
                final Object value = ((Map<?, ?>) current).get(var);
                if (value != null) {
                    addValue(var, value);
                    continue;
                }
            }

            final TraversalProduct product =
                    TraversalUtil.produce((S) this.getNullableScopeValue(Pop.last, var, traverser), this.traversalRing.next());

            if (!product.isProductive() || product.get() == null) {
                productive = false;
                break;
            }

            addValue(var, product.get());
        }
        this.traversalRing.reset();

        return productive ?
                PathProcessor.processTraverserPathLabels(traverser.split(evaluate(), this), this.keepLabels) :
                EmptyTraverser.instance();
    }

    private void addValue(final String variableName, final Object value) {
        values.put(variableName, value instanceof String ? (String) value : value.toString());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.format, this.traversalRing);
    }

    @Override
    public int hashCode() {
        return (super.hashCode() * 31 + this.format.hashCode()) * 31 + this.traversalRing.hashCode();
    }

    @Override
    public List<Traversal.Admin<S, String>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public FormatStep<S> clone() {
        final FormatStep<S> clone = (FormatStep<S>) super.clone();
        clone.format = this.format;
        clone.variables = this.variables;
        clone.traversalRing = this.traversalRing;
        clone.matcher = this.matcher;
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
        return variables;
    }

    @Override
    public void setKeepLabels(final Set<String> labels) {
        this.keepLabels = labels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    // private methods

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("%\\{(.*?)\\}");

    Set<String> getVariables() {
        final Set<String> variables = new LinkedHashSet<>();
        while (matcher.find()) {
            variables.add(matcher.group(1));
        }
        return variables;
    }

    private String evaluate() {
        int lastIndex = 0;
        final StringBuilder output = new StringBuilder();
        matcher.reset();

        while (matcher.find()) {
            output.append(format, lastIndex, matcher.start()).append(values.get(matcher.group(1)));

            lastIndex = matcher.end();
        }

        if (lastIndex < format.length()) {
            output.append(format, lastIndex, format.length());
        }
        return output.toString();
    }
}
