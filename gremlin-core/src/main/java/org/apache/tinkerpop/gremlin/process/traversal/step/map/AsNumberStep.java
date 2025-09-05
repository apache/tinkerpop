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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.tinkerpop.gremlin.process.traversal.N;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.GType;
import org.apache.tinkerpop.gremlin.structure.GremlinDataType;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for number parsing step.
 */
public class AsNumberStep<S> extends ScalarMapStep<S, Number> {

    private N numberToken;
    private GremlinDataType gDT;
    private Class<?> clazz;

    public AsNumberStep(final Traversal.Admin traversal) {
        super(traversal);
        this.numberToken = null;
    }

    public AsNumberStep(final Traversal.Admin traversal, final N numberToken) {
        super(traversal);
        this.numberToken = numberToken;
    }

    public AsNumberStep(final Traversal.Admin traversal, final GremlinDataType numberToken) {
        super(traversal);
        this.gDT = numberToken;
    }

    public AsNumberStep(final Traversal.Admin traversal, final Class<?> numberToken) {
        super(traversal);
        this.gDT = GType.valueOf(numberToken.getSimpleName().toUpperCase());
    }

    @Override
    protected Number map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();
        if (object instanceof String) {
            String numberText = (String) object;
            Number number = parseNumber(numberText);
            return numberToken == null ? (gDT == null ? number : castNumber(number, gDT))
                    : castNumber(number, numberToken);
//            return numberToken == null ? number : castNumber(number, numberToken);
        } else if (object instanceof Number) {
            Number number = (Number) object;
            return numberToken == null ? (gDT == null ? number : castNumber(number, gDT))
                    : castNumber(number, numberToken);
//            return numberToken == null ? number : castNumber(number, numberToken);
        }
        throw new IllegalArgumentException(String.format("Can't parse type %s as number.", object == null ? "null" : object.getClass().getSimpleName()));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (numberToken != null ? numberToken.hashCode() : 0);
        return result;
    }

    @Override
    public AsNumberStep<S> clone() {
        final AsNumberStep<S> clone = (AsNumberStep<S>) super.clone();
        clone.numberToken = this.numberToken;
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    private static Number parseNumber(final String value) {
        if (NumberUtils.isCreatable(value.trim())) {
            return NumberUtils.createNumber(value.trim());
        }
        throw new NumberFormatException(String.format("Can't parse string '%s' as number.", value));
    }

    private static Number castNumber(final Number number, final N numberToken) {
        return NumberHelper.castTo(number, numberToken);
    }

    private static Number castNumber(final Number number, final GremlinDataType numberToken) {
        return NumberHelper.castTo(number, numberToken);
    }

}
