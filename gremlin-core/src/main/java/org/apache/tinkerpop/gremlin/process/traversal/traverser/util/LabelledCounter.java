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

package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.commons.lang.mutable.MutableShort;

import java.io.Serializable;

/**
 * Class to track a count associated with a Label
 */
public class LabelledCounter implements Serializable, Cloneable {

    private final String label;
    private final MutableShort count = new MutableShort();

    protected LabelledCounter() {
        label = "";
    }

    public LabelledCounter(final String label, final short initialCount) {
        if (label == null) {
            throw new NullPointerException("Label is null");
        }
        this.label = label;
        this.count.setValue(initialCount);
    }

    public boolean hasLabel(final String label){
        return this.label.equals(label);
    }

    public int count() {
        return this.count.intValue();
    }

    public void increment() {
        this.count.increment();
    }

    @Override
    public Object clone() {
        return new LabelledCounter(this.label, this.count.shortValue());
    }

    @Override
    public String toString(){
        return "Step Label: " + label + " Counter: " + count.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof LabelledCounter)) return false;

        LabelledCounter that = (LabelledCounter) o;

        if (!this.label.equals(that.label)) return false;
        return this.count.equals(that.count);
    }

    @Override
    public int hashCode() {
        int result = this.label.hashCode();
        result = 31 * result + this.count.hashCode();
        return result;
    }
}
