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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.util.function.ScriptEngineLambda;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineSuite.ENGINE_TO_TEST;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ScriptEngineLambdaTest {

    @Test
    public void shouldCallAsFunction() {
        final ScriptEngineLambda lambda = new ScriptEngineLambda(ENGINE_TO_TEST, "1+a");
        assertEquals(11, Integer.parseInt(lambda.apply(10).toString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnBadScriptAsFunction() {
        new ScriptEngineLambda(ENGINE_TO_TEST, "1432423)a").apply("a");
    }

    @Test
    public void shouldCallAsSupplier() {
        final ScriptEngineLambda lambda = new ScriptEngineLambda(ENGINE_TO_TEST, "11");
        assertEquals(11, lambda.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnBadScriptAsSupplier() {
        new ScriptEngineLambda(ENGINE_TO_TEST, "1432423)a").get();
    }

    @Test
    public void shouldCallAsPredicate() {
        final ScriptEngineLambda lambda = new ScriptEngineLambda(ENGINE_TO_TEST, "a > 10");
        assertThat(lambda.test(100), is(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnBadScriptAsPredicate() {
        new ScriptEngineLambda(ENGINE_TO_TEST, "1432423)a").test(1);
    }

    @Test
    public void shouldCallAsConsumer() {
        final ScriptEngineLambda lambda = new ScriptEngineLambda(ENGINE_TO_TEST, "a.setData('test')");
        final Junk junk = new Junk();
        lambda.accept(junk);
        assertEquals("test", junk.getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnBadScriptAsConsumer() {
        new ScriptEngineLambda(ENGINE_TO_TEST, "1432423)a").accept("1");
    }

    @Test
    public void shouldCallAsBiConsumer() {
        final ScriptEngineLambda lambda = new ScriptEngineLambda(ENGINE_TO_TEST, "a.setData('testa');b.setData('testb')");
        final Junk junkA = new Junk();
        final Junk junkB = new Junk();
        lambda.accept(junkA, junkB);

        assertEquals("testa", junkA.getData());
        assertEquals("testb", junkB.getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnBadScriptAsBiConsumer() {
        new ScriptEngineLambda(ENGINE_TO_TEST, "1432423)a").accept("1", "2");
    }

    @Test
    public void shouldCallAsTriConsumer() {
        final ScriptEngineLambda lambda = new ScriptEngineLambda(ENGINE_TO_TEST, "a.setData('testa');b.setData('testb');c.setData('testc')");
        final Junk junkA = new Junk();
        final Junk junkB = new Junk();
        final Junk junkC = new Junk();
        lambda.accept(junkA, junkB, junkC);

        assertEquals("testa", junkA.getData());
        assertEquals("testb", junkB.getData());
        assertEquals("testc", junkC.getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnBadScriptAsTriConsumer() {
        new ScriptEngineLambda(ENGINE_TO_TEST, "1432423)a").accept("1", "2", "3");
    }

    public static class Junk {
        private String data = "";

        public String getData() {
            return data;
        }

        public void setData(final String x) {
            data = x;
        }
    }
}