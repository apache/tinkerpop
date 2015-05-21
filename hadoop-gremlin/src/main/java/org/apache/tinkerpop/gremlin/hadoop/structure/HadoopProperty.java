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
package org.apache.tinkerpop.gremlin.hadoop.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopProperty<V> implements Property<V>, WrappedProperty<Property<V>> {

    private final Property<V> baseProperty;
    private final Element hadoopElement;

    protected HadoopProperty(final Property<V> baseProperty, final Element hadoopElement) {
        this.baseProperty = baseProperty;
        this.hadoopElement = hadoopElement;
    }

    @Override
    public boolean isPresent() {
        return this.baseProperty.isPresent();
    }

    @Override
    public V value() {
        return this.baseProperty.value();
    }

    @Override
    public Property<V> getBaseProperty() {
        return this.baseProperty;
    }

    @Override
    public String key() {
        return this.baseProperty.key();
    }

    @Override
    public void remove() {
        this.baseProperty.remove();
    }

    @Override
    public Element element() {
        return this.hadoopElement;
    }

    @Override
    public boolean equals(final Object object) {
        return this.baseProperty.equals(object);
    }

    @Override
    public int hashCode() {
        return this.baseProperty.hashCode();
    }

    @Override
    public String toString() {
        return this.baseProperty.toString();
    }
}
