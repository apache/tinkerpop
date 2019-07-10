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
package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StaticMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
    public MapReduce<MK, MV, RK, RV, R> clone() {
        return this;
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.getMemoryKey());
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + this.getMemoryKey()).hashCode();
    }
}
