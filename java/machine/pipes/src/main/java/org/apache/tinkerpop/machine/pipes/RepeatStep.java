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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.functions.branch.RepeatBranch;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.util.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatStep<C, S> extends AbstractStep<C, S, S> {

    private final Compilation<C, S, ?> until;
    private final Compilation<C, S, S> repeat;
    private Iterator<Traverser<C, S>> nextTraversers;

    RepeatStep(final Step<C, ?, S> previousStep, final RepeatBranch<C, S> repeatFunction) {
        super(previousStep, repeatFunction);
        this.until = repeatFunction.getUntil();
        this.repeat = repeatFunction.getRepeat();
        this.nextTraversers = IteratorUtils.filter(this.repeat.getProcessor(), t -> {
            if (!this.until.filterTraverser(t)) {
                this.repeat.getProcessor().addStart(t);
                return false;
            } else
                return true;
        });
    }

    @Override
    public boolean hasNext() {
        this.stageOutput();
        return this.nextTraversers.hasNext();
    }

    @Override
    public Traverser<C, S> next() {
        this.stageOutput();
        return this.nextTraversers.next();
    }

    private final void stageOutput() {
        while (!this.nextTraversers.hasNext() && this.previousStep.hasNext()) {
            this.repeat.addTraverser(super.previousStep.next());
        }

    }

    @Override
    public void reset() {
        this.nextTraversers = Collections.emptyIterator();
    }
}

