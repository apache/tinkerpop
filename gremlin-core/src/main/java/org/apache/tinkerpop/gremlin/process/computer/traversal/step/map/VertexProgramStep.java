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

package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.VertexComputing;
import org.apache.tinkerpop.gremlin.process.computer.util.EmptyMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class VertexProgramStep extends AbstractStep<ComputerResult, ComputerResult> implements VertexComputing {

    public static final String ROOT_TRAVERSAL = "gremlin.vertexProgramStep.rootTraversal";
    public static final String STEP_ID = "gremlin.vertexProgramStep.stepId";

    protected Computer computer = Computer.compute();

    protected boolean first = true;

    public VertexProgramStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<ComputerResult> processNextStart() throws NoSuchElementException {
        Future<ComputerResult> future = null;
        try {
            if (this.first && this.getPreviousStep() instanceof EmptyStep) {
                this.first = false;
                final Graph graph = this.getTraversal().getGraph().get();
                future = this.getComputer().apply(graph).program(this.generateProgram(graph, EmptyMemory.instance())).submit();
                final ComputerResult result = future.get();
                this.processMemorySideEffects(result.memory());
                return this.getTraversal().getTraverserGenerator().generate(result, this, 1l);
            } else {
                final Traverser.Admin<ComputerResult> traverser = this.starts.next();
                final Graph graph = traverser.get().graph();
                final Memory memory = traverser.get().memory();
                future = this.getComputer().apply(graph).program(this.generateProgram(graph, memory)).submit();
                final ComputerResult result = future.get();
                this.processMemorySideEffects(result.memory());
                return traverser.split(result, this);
            }
        } catch (final InterruptedException ie) {
            // the thread running the traversal took an interruption while waiting on the call the future.get().
            // the future should then be cancelled with interruption so that the GraphComputer that created
            // the future knows we don't care about it anymore. The GraphComputer should attempt to respect this
            // cancellation request.
            if (future != null) future.cancel(true);
            throw new TraversalInterruptedException();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Computer getComputer() {
        Computer tempComputer = this.computer;
        if (!this.isEndStep()) {
            if (null == tempComputer.getPersist())
                tempComputer = tempComputer.persist(GraphComputer.Persist.EDGES);
            if (null == tempComputer.getResultGraph())
                tempComputer = tempComputer.result(GraphComputer.ResultGraph.NEW);
        }
        return tempComputer;
    }

    @Override
    public void setComputer(final Computer computer) {
        this.computer = computer;
    }

    protected boolean previousTraversalVertexProgram() {
        Step<?, ?> currentStep = this;
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof TraversalVertexProgramStep)
                return true;
            currentStep = currentStep.getPreviousStep();
        }
        return false;
    }

    private void processMemorySideEffects(final Memory memory) {
        // update the traversal side-effects with the state of the memory after the OLAP job execution
        final TraversalSideEffects sideEffects = this.getTraversal().getSideEffects();
        for (final String key : memory.keys()) {
            if (sideEffects.exists(key)) {
                // halted traversers should never be propagated through sideEffects
                assert !key.equals(TraversalVertexProgram.HALTED_TRAVERSERS);
                sideEffects.set(key, memory.get(key));
            }
        }
    }

    protected boolean isEndStep() {
        return this.getNextStep() instanceof ComputerResultStep || (this.getNextStep() instanceof ProfileStep && this.getNextStep().getNextStep() instanceof ComputerResultStep);
    }

}
