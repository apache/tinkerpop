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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TestMessagePrioritiesActorProgram implements ActorProgram {

    @Override
    public Map<Class, BinaryOperator> getMessageTypes() {
        final Map<Class, BinaryOperator> messages = new LinkedHashMap<>();
        messages.put(Integer.class, null);
        messages.put(Double.class, null);
        messages.put(Long.class, null);
        return messages;
    }

    @Override
    public Worker createWorkerProgram(final Actor.Worker worker) {
        return new Worker<Number>() {

            private int longCount = 0;

            @Override
            public void setup() {
                worker.send(worker.master(), 2.0d);
                worker.send(worker.master(), 1);
            }

            @Override
            public void execute(final Number message) {
                assertEquals(5L, message);
                assertEquals(0, this.longCount);
                this.longCount++;
                worker.send(worker.master(), message);
            }

            @Override
            public void terminate() {
                assertEquals(1, this.longCount);
            }
        };
    }

    @Override
    public Master createMasterProgram(final Actor.Master master) {

        return new Master<Number>() {

            private int doubleCount = 0;
            private int integerCount = 0;
            private int longCount = 0;


            @Override
            public void setup() {
                try {
                    Thread.sleep(10);
                } catch (Exception e) {

                }
            }

            @Override
            public void execute(final Number message) {
                if (message instanceof Integer) {
                    assertEquals(1, message);
                    assertEquals(0, this.doubleCount);
                    assertEquals(0, this.longCount);
                    this.integerCount++;
                } else if (message instanceof Double) {
                    assertEquals(2.0d, message);
                    assertEquals(master.workers().size(), this.integerCount);
                    assertEquals(0, this.longCount);
                    if (this.doubleCount == 0)
                        master.broadcast(5L);
                    this.doubleCount++;
                } else if (message instanceof Long) {
                    assertEquals(5L, message);
                    assertEquals(master.workers().size(), this.integerCount);
                    assertEquals(master.workers().size(), this.doubleCount);
                    if (++this.longCount == master.workers().size())
                        master.close();
                }
            }

            @Override
            public void terminate() {
                assertEquals(master.workers().size(), this.integerCount);
                assertEquals(master.workers().size(), this.doubleCount);
                assertEquals(master.workers().size(), this.longCount);
                master.setResult(null);
            }
        };
    }

    @Override
    public ActorProgram clone() {
        return this;
    }
}
