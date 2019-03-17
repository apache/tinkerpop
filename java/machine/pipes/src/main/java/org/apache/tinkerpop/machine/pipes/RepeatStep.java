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
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class RepeatStep<C, S> extends AbstractStep<C, S, S> {

    private final RepeatBranch<C, S> repeatBranch;
    private final int untilLocation;
    private final int emitLocation;
    private final Compilation<C, S, ?> untilCompilation;
    private final Compilation<C, S, ?> emitCompilation;
    private final Compilation<C, S, S> repeat;
    private TraverserSet<C, S> outputTraversers = new TraverserSet<>();
    private TraverserSet<C, S> inputTraversers = new TraverserSet<>();
    private final boolean hasStartPredicates;
    private final boolean hasEndPredicates;

    RepeatStep(final Step<C, ?, S> previousStep, final RepeatBranch<C, S> repeatBranch) {
        super(previousStep, repeatBranch);
        this.repeatBranch = repeatBranch;
        this.untilCompilation = repeatBranch.getUntil();
        this.emitCompilation = repeatBranch.getEmit();
        this.repeat = repeatBranch.getRepeat();
        this.untilLocation = repeatBranch.getUntilLocation();
        this.emitLocation = repeatBranch.getEmitLocation();
        this.hasStartPredicates = repeatBranch.hasStartPredicates();
        this.hasEndPredicates = repeatBranch.hasEndPredicates();
    }

    @Override
    public boolean hasNext() {
        this.stageOutput();
        return !this.outputTraversers.isEmpty();
    }

    @Override
    public Traverser<C, S> next() {
        this.stageOutput();
        return this.outputTraversers.remove();
    }

    private final void stageInput() {
        if (this.hasStartPredicates) {
            final Traverser<C, S> traverser = this.inputTraversers.isEmpty() ? this.previousStep.next() : this.inputTraversers.remove();
            if (1 == this.untilLocation) {
                if (this.untilCompilation.filterTraverser(traverser.clone())) {
                    this.outputTraversers.add(traverser);
                } else if (2 == this.emitLocation && this.emitCompilation.filterTraverser(traverser.clone())) {
                    this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                    this.repeat.addTraverser(traverser);
                } else
                    this.repeat.addTraverser(traverser);
            } else if (1 == this.emitLocation) {
                if (this.emitCompilation.filterTraverser(traverser.clone()))
                    this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                if (2 == this.untilLocation && this.untilCompilation.filterTraverser(traverser.clone()))
                    this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                else
                    this.repeat.addTraverser(traverser);
            }
        } else {
            this.repeat.addTraverser(this.inputTraversers.isEmpty() ? this.previousStep.next() : this.inputTraversers.remove());
        }
    }

    private final void stageOutput() {
        while (this.outputTraversers.isEmpty() && (this.previousStep.hasNext() || !this.inputTraversers.isEmpty())) {
            this.stageInput();
            if (this.repeat.getProcessor().hasNext()) {
                final Traverser<C, S> traverser = this.repeat.getProcessor().next();
                if (this.hasEndPredicates) {
                    if (3 == this.untilLocation) {
                        if (this.untilCompilation.filterTraverser(traverser.clone())) {
                            this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                        } else if (4 == this.emitLocation && this.emitCompilation.filterTraverser(traverser.clone())) {
                            this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                            this.inputTraversers.add(traverser.repeatLoop(this.repeatBranch));
                        } else
                            this.inputTraversers.add(traverser.repeatLoop(this.repeatBranch));
                    } else if (3 == this.emitLocation) {
                        if (this.emitCompilation.filterTraverser(traverser.clone()))
                            this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                        if (4 == this.untilLocation && this.untilCompilation.filterTraverser(traverser.clone()))
                            this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                        else
                            this.inputTraversers.add(traverser.repeatLoop(this.repeatBranch));
                    }
                } else {
                    this.inputTraversers.add(traverser.repeatLoop(this.repeatBranch));
                }
            }
        }
    }

    @Override
    public void reset() {
        this.inputTraversers.clear();
        this.outputTraversers.clear();
    }
}

