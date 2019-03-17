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
package org.apache.tinkerpop.machine.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.traverser.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RepeatEndFn<C, S> extends AbstractFn<C, S, S> {

    private final RepeatBranch<C, S> repeatBranch;
    private final int untilLocation;
    private final int emitLocation;
    private final Compilation<C, S, ?> untilCompilation;
    private final Compilation<C, S, ?> emitCompilation;
    private final TupleTag<Traverser<C, S>> repeatDone;
    private final TupleTag<Traverser<C, S>> repeatLoop;


    public RepeatEndFn(final RepeatBranch<C, S> repeatBranch,
                       final TupleTag<Traverser<C, S>> repeatDone,
                       final TupleTag<Traverser<C, S>> repeatLoop) {
        super(repeatBranch);
        this.repeatBranch = repeatBranch;
        this.untilLocation = repeatBranch.getUntilLocation();
        this.untilCompilation = repeatBranch.getUntil();
        this.emitLocation = repeatBranch.getEmitLocation();
        this.emitCompilation = repeatBranch.getEmit();
        this.repeatDone = repeatDone;
        this.repeatLoop = repeatLoop;
    }

    @ProcessElement
    public void processElement(final @DoFn.Element Traverser<C, S> traverser, final MultiOutputReceiver out) {
        if (3 == this.untilLocation) {
            if (this.untilCompilation.filterTraverser(traverser)) {
                out.get(this.repeatDone).output(traverser.repeatDone(this.repeatBranch));
            } else if (4 == this.emitLocation && this.emitCompilation.filterTraverser(traverser)) {
                out.get(this.repeatDone).output(traverser.repeatDone(this.repeatBranch));
                out.get(this.repeatLoop).output(traverser.repeatLoop(this.repeatBranch));
            } else {
                out.get(this.repeatLoop).output(traverser.repeatLoop(this.repeatBranch));
            }
        } else if (3 == this.emitLocation) {
            if (this.emitCompilation.filterTraverser(traverser))
                out.get(this.repeatDone).output(traverser.repeatDone(this.repeatBranch));
            if (4 == this.untilLocation && this.untilCompilation.filterTraverser(traverser))
                out.get(this.repeatDone).output(traverser.repeatDone(this.repeatBranch));
            else
                out.get(this.repeatLoop).output(traverser.repeatLoop(this.repeatBranch));
        }
    }
}
