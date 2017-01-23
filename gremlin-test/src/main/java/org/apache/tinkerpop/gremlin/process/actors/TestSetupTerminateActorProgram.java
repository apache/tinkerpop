/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.actors;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TestSetupTerminateActorProgram implements ActorProgram {

    private static final String WORKER_SETUP = "workerSetup";
    private static final String WORKER_TERMINATE = "workerTerminate";
    private static final String MASTER_SETUP = "masterSetup";
    private static final String MASTER_TERMINATE = "masterTerminate";

    @Override
    public Worker createWorkerProgram(final Actor.Worker worker) {
        return new Worker<String>() {

            private boolean hasSetup = false;

            @Override
            public void setup() {
                worker.send(worker.master(), WORKER_SETUP);
                assertFalse(this.hasSetup);
                this.hasSetup = true;
                worker.close();
            }

            @Override
            public void execute(final String message) {
                fail("The worker should not have received any messages");
            }

            @Override
            public void terminate() {
                assertTrue(this.hasSetup);
                worker.send(worker.master(), WORKER_TERMINATE);
            }
        };
    }

    @Override
    public Master createMasterProgram(Actor.Master master) {
        return new Master<String>() {
            private int masterSetup = 0;
            private int masterTerminate = 0;
            private int workerSetup = 0;
            private int workerTerminate = 0;

            @Override
            public void setup() {
                master.send(master.address(), MASTER_SETUP);
                try {
                    Thread.sleep(250);
                } catch (final Exception e) {
                }
                master.send(master.address(), "kill");
            }

            @Override
            public void execute(final String message) {
                if (message.equals(WORKER_SETUP)) {
                    this.workerSetup++;
                    assertTrue(this.workerSetup > this.workerTerminate);
                } else if (message.equals(WORKER_TERMINATE)) {
                    this.workerTerminate++;
                    assertTrue(this.workerTerminate <= this.workerSetup);
                } else if (message.equals(MASTER_SETUP)) {
                    assertEquals(0, this.masterSetup);
                    assertEquals(0, this.masterTerminate);
                    this.masterSetup++;
                } else {
                    assertEquals("kill", message);
                    assertEquals(1, this.masterSetup);
                    assertEquals(0, this.masterTerminate);
                    assertEquals(this.workerSetup, this.workerTerminate);
                    master.close();
                }
            }

            @Override
            public void terminate() {
                assertEquals(this.workerSetup, this.workerTerminate);
                assertEquals(1, this.masterSetup);
                assertEquals(0, this.masterTerminate);
                master.result().setResult(Arrays.asList(
                        this.workerSetup,
                        this.workerTerminate,
                        this.masterSetup,
                        ++this.masterTerminate));
            }
        };
    }

    @Override
    public ActorProgram clone() {
        return this;
    }
}
