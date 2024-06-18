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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Objects;

public final class CardinalityValueTraversal extends AbstractLambdaTraversal {

    private final VertexProperty.Cardinality cardinality;

    private final Object value;

    private final GremlinLang gremlinLang;

    public CardinalityValueTraversal(final VertexProperty.Cardinality cardinality, final Object value) {
        this.cardinality = cardinality;
        this.value = value;
        this.gremlinLang = new GremlinLang(CardinalityValueTraversal.class.getSimpleName(), cardinality.name(), value);
    }

    @Override
    public GremlinLang getGremlinLang() {
        return this.gremlinLang;
    }

    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "[" + cardinality + ", " + value + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof CardinalityValueTraversal)) return false;
        if (!super.equals(o)) return false;

        final CardinalityValueTraversal that = (CardinalityValueTraversal) o;

        if (cardinality != that.cardinality) return false;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + cardinality.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
