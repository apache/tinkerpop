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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.StringLocalStep;

/**
 * Reference implementation for rTrim() step, a mid-traversal step which a string with trailing
 * whitespace removed. Null values are not processed and remain as null when returned.
 * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class RTrimLocalStep<S, E> extends StringLocalStep<S, E> {

    public RTrimLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected E applyStringOperation(String item) {
        return (E) item.substring(0,getEndIdx(item)+1);
    }

    @Override
    public String getStepName() { return "rTrim(local)"; }

    private int getEndIdx(final String str) {
        int idx = str.length() - 1;
        while (idx >= 0 && Character.isWhitespace(str.charAt(idx))) {
            idx--;
        }
        return idx;
    }

}
