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
import org.apache.tinkerpop.machine.functions.branch.RepeatBranch;
import org.apache.tinkerpop.machine.traversers.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RepeatFn<C, S> extends AbstractFn<C, S, S> {

    private final Compilation<C, S, ?> until;
    private final TupleTag repeatDone;
    private final TupleTag repeatLoop;
    private final boolean deadEnd;


    public RepeatFn(final RepeatBranch<C, S> repeatBranch, final TupleTag repeatDone, final TupleTag repeatLoop, final boolean deadEnd) {
        super(repeatBranch);
        this.until = repeatBranch.getUntil();
        this.repeatDone = repeatDone;
        this.repeatLoop = repeatLoop;
        this.deadEnd = deadEnd;
    }

    @ProcessElement
    public void processElement(final @DoFn.Element Traverser<C, S> traverser, final MultiOutputReceiver out) {
        if (this.until.filterTraverser(traverser.clone()))
            out.get(this.repeatDone).output(traverser.clone());
        else if (!this.deadEnd)
            out.get(this.repeatLoop).output(traverser.clone());
        else
            throw new IllegalStateException("There are not enough repetition to account for this traveral");
    }
}
