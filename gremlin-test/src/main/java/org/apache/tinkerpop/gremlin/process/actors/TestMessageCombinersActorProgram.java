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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TestMessageCombinersActorProgram implements ActorProgram {

    @Override
    public Map<Class, BinaryOperator> getMessageTypes() {
        final Map<Class, BinaryOperator> messages = new LinkedHashMap<>();
        messages.put(Integer.class, Operator.sum);
        messages.put(Double.class, null);
        messages.put(Long.class, Operator.max);
        return messages;
    }

    @Override
    public Worker createWorkerProgram(final Actor.Worker worker) {
        return new Worker<Number>() {

            private int doubleCount = 0;

            @Override
            public void setup() {

            }

            @Override
            public void execute(final Number message) {
                worker.send(worker.master(), message);
                if (message instanceof Integer) {
                    assertEquals(1, message);
                    worker.broadcast(1.0d);
                } else if (message instanceof Double) {
                    assertEquals(1.0d, message);
                    this.doubleCount++;
                }
            }

            @Override
            public void terminate() {
                assertEquals(worker.workers().size(), this.doubleCount);
            }
        };
    }

    @Override
    public Master createMasterProgram(final Actor.Master master) {

        return new Master<Number>() {

            private long doubleCount = 0L;
            private boolean seenDouble = false;
            private boolean seenInteger = false;
            private boolean seenLong = false;


            @Override
            public void setup() {
                master.broadcast(1);
                try {
                    Thread.sleep(10);
                } catch (Exception e) {

                }
            }

            @Override
            public void execute(final Number message) {
                if (message instanceof Integer) {
                    assertEquals(master.workers().size(), message);
                    assertFalse(this.seenInteger);
                    assertFalse(this.seenLong);
                    this.seenInteger = true;
                } else if (message instanceof Double) {
                    this.doubleCount++;
                    assertEquals(1.0d, message);
                    assertFalse(this.seenLong);
                    this.seenDouble = true;
                    master.broadcast(this.doubleCount);
                    try {
                        Thread.sleep(10);
                    } catch (Exception e) {

                    }
                } else if (message instanceof Long) {
                    assertEquals((long) Math.pow(master.workers().size(), 2.0d), message);
                    assertTrue(this.seenDouble);
                    assertTrue(this.seenInteger);
                    assertFalse(this.seenLong);
                    this.seenLong = true;
                    master.close();
                }
            }

            @Override
            public void terminate() {
                assertEquals((long) Math.pow(master.workers().size(), 2.0d), this.doubleCount);
                assertTrue(this.seenDouble);
                assertTrue(this.seenInteger);
                assertTrue(this.seenLong);
                master.setResult(null);
            }
        };
    }

    @Override
    public ActorProgram clone() {
        return this;
    }
}
