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
package org.apache.tinkerpop.gremlin.process.traverser;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.traverser.util.AbstractPathTraverser;
import org.apache.tinkerpop.gremlin.process.util.path.ImmutablePath;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_O_P_PA_S_SE_SL_Traverser<T> extends AbstractPathTraverser<T> {

    protected B_O_P_PA_S_SE_SL_Traverser() {
    }

    public B_O_P_PA_S_SE_SL_Traverser(final T t, final Step<T, ?> step) {
        super(t, step);
        final Optional<String> stepLabel = step.getLabel();
        this.path = stepLabel.isPresent() ?
                ImmutablePath.make().extend(t, stepLabel.get()) :
                ImmutablePath.make().extend(t);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.path.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (object instanceof B_O_P_PA_S_SE_SL_Traverser)
                && ((B_O_P_PA_S_SE_SL_Traverser) object).path().equals(this.path) // TODO: path equality
                && ((B_O_P_PA_S_SE_SL_Traverser) object).get().equals(this.t)
                && ((B_O_P_PA_S_SE_SL_Traverser) object).getStepId().equals(this.getStepId())
                && ((B_O_P_PA_S_SE_SL_Traverser) object).loops() == this.loops()
                && (null == this.sack);
    }

}
