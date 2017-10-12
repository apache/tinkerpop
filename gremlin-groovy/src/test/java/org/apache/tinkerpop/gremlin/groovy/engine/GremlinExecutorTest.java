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
package org.apache.tinkerpop.gremlin.groovy.engine;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.javatuples.Pair;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutorTest {
    public static Map<String, String> PATHS = new HashMap<>();
    private final BasicThreadFactory testingThreadFactory = new BasicThreadFactory.Builder().namingPattern("test-gremlin-executor-%d").build();

    /**
     * Temporary "useless" plugin definition to force GremlinExecutor to use GremlinScriptEngineManager - will be
     * removed when the old functionality of ScriptEngines is removed.
     */
    private final Map<String, Map<String,Object>> triggerPlugin = new HashMap<String, Map<String,Object>>() {{
        put(ImportGremlinPlugin.class.getName(), new HashMap<String,Object>() {{
            put("classImports", Collections.singletonList("java.lang.Math"));
        }});
    }};

    private final Map<String, Map<String,Object>> scriptFilePlugin = new HashMap<String, Map<String,Object>>() {{
        put(ScriptFileGremlinPlugin.class.getName(), new HashMap<String,Object>() {{
            put("files", Collections.singletonList(PATHS.get("GremlinExecutorInit.groovy")));
        }});
    }};

    static {
        try {
            final List<String> groovyScriptResources = Collections.singletonList("GremlinExecutorInit.groovy");
            for (final String fileName : groovyScriptResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(GremlinExecutorTest.class, fileName, "").getAbsolutePath());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldRaiseExceptionInWithResultOfLifeCycle() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final GremlinExecutor.LifeCycle lc = GremlinExecutor.LifeCycle.build()
                .withResult(r -> {
                    throw new RuntimeException("no worky");
                }).create();

        final AtomicBoolean exceptionRaised = new AtomicBoolean(false);

        final CompletableFuture<Object> future = gremlinExecutor.eval("1+1", "gremlin-groovy", new SimpleBindings(), lc);
        future.handle((r, t) -> {
            exceptionRaised.set(t != null && t instanceof RuntimeException && t.getMessage().equals("no worky"));
            return null;
        }).get();

        assertThat(exceptionRaised.get(), is(true));

        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScript() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        assertEquals(2, gremlinExecutor.eval("1+1").get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldCompileScript() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final CompiledScript script = gremlinExecutor.compile("1+1").get();
        assertEquals(2, script.eval());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalSuccessfulAssertionScript() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        assertNull(gremlinExecutor.eval("assert 1==1").get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalFailingAssertionScript() throws Exception {
        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build().create()) {
            gremlinExecutor.eval("assert 1==0").get();
            fail("Should have thrown an exception");
        } catch (Exception ex) {
            assertThat(ex.getCause(), instanceOf(AssertionError.class));
        }
    }

    @Test
    public void shouldEvalMultipleScripts() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        assertEquals(2, gremlinExecutor.eval("1+1").get());
        assertEquals(3, gremlinExecutor.eval("1+2").get());
        assertEquals(4, gremlinExecutor.eval("1+3").get());
        assertEquals(5, gremlinExecutor.eval("1+4").get());
        assertEquals(6, gremlinExecutor.eval("1+5").get());
        assertEquals(7, gremlinExecutor.eval("1+6").get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithBindings() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final Bindings b = new SimpleBindings();
        b.put("x", 1);
        assertEquals(2, gremlinExecutor.eval("1+x", b).get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithMapBindings() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final Map<String,Object> b = new HashMap<>();
        b.put("x", 1);
        assertEquals(2, gremlinExecutor.eval("1+x", b).get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithMapBindingsAndLanguage() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final Map<String,Object> b = new HashMap<>();
        b.put("x", 1);
        assertEquals(2, gremlinExecutor.eval("1+x", "gremlin-groovy", b).get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithMapBindingsAndLanguageThenTransform() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final Map<String,Object> b = new HashMap<>();
        b.put("x", 1);
        assertEquals(4, gremlinExecutor.eval("1+x", "gremlin-groovy", b, r -> (int) r * 2).get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithMapBindingsAndLanguageThenConsume() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final Map<String,Object> b = new HashMap<>();
        b.put("x", 1);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger result = new AtomicInteger(0);
        assertEquals(2, gremlinExecutor.eval("1+x", "gremlin-groovy", b, r -> {
            result.set((int) r * 2);
            latch.countDown();
        }).get());

        latch.await();
        assertEquals(4, result.get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithGlobalBindings() throws Exception {
        final Bindings b = new SimpleBindings();
        b.put("x", 1);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(b).create();
        assertEquals(2, gremlinExecutor.eval("1+x").get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldGetGlobalBindings() throws Exception {
        final Bindings b = new SimpleBindings();
        final Object bound = new Object();
        b.put("x", bound);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().
                addPlugins("gremlin-groovy", triggerPlugin).
                globalBindings(b).create();
        assertEquals(bound, gremlinExecutor.getScriptEngineManager().getBindings().get("x"));
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithGlobalAndLocalBindings() throws Exception {
        final Bindings g = new SimpleBindings();
        g.put("x", 1);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(g).create();
        final Bindings b = new SimpleBindings();
        b.put("y", 1);
        assertEquals(2, gremlinExecutor.eval("y+x", b).get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldEvalScriptWithLocalOverridingGlobalBindings() throws Exception {
        final Bindings g = new SimpleBindings();
        g.put("x", 1);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(g).create();
        final Bindings b = new SimpleBindings();
        b.put("x", 10);
        assertEquals(11, gremlinExecutor.eval("x+1", b).get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldTimeoutSleepingScript() throws Exception {
        final AtomicBoolean successCalled = new AtomicBoolean(false);
        final AtomicBoolean failureCalled = new AtomicBoolean(false);

        final CountDownLatch timeOutCount = new CountDownLatch(1);

        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .scriptEvaluationTimeout(250)
                .afterFailure((b, e) -> failureCalled.set(true))
                .afterSuccess((b) -> successCalled.set(true))
                .afterTimeout((b) -> timeOutCount.countDown()).create();
        try {
            gremlinExecutor.eval("Thread.sleep(1000);10").get();
            fail("This script should have timed out with an exception");
        } catch (Exception ex) {
            assertEquals(TimeoutException.class, ex.getCause().getClass());
        }

        assertTrue(timeOutCount.await(2000, TimeUnit.MILLISECONDS));

        assertFalse(successCalled.get());
        assertFalse(failureCalled.get());
        assertEquals(0, timeOutCount.getCount());
        gremlinExecutor.close();
    }

    @Test
    public void shouldTimeoutSleepingScriptViaOverrideOnLifeCycle() throws Exception {
        final AtomicBoolean successCalled = new AtomicBoolean(false);
        final AtomicBoolean failureCalled = new AtomicBoolean(false);

        final CountDownLatch timeOutCount = new CountDownLatch(1);

        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .scriptEvaluationTimeout(10000)
                .afterFailure((b, e) -> failureCalled.set(true))
                .afterSuccess((b) -> successCalled.set(true))
                .afterTimeout((b) -> timeOutCount.countDown()).create();
        try {
            final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build()
                    .scriptEvaluationTimeoutOverride(250L).create();
            gremlinExecutor.eval("Thread.sleep(1000);10", "gremlin-groovy", new SimpleBindings(), lifeCycle).get();
            fail("This script should have timed out with an exception");
        } catch (Exception ex) {
            assertEquals(TimeoutException.class, ex.getCause().getClass());
        }

        assertTrue(timeOutCount.await(2000, TimeUnit.MILLISECONDS));

        assertFalse(successCalled.get());
        assertFalse(failureCalled.get());
        assertEquals(0, timeOutCount.getCount());
        gremlinExecutor.close();
    }

    @Test
    public void shouldOverrideBeforeEval() throws Exception {
        final AtomicInteger called = new AtomicInteger(0);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().beforeEval(b -> called.set(1)).create();
        assertEquals(2, gremlinExecutor.eval("1+1", null, new SimpleBindings(),
                GremlinExecutor.LifeCycle.build().beforeEval(b -> called.set(200)).create()).get());

        // need to wait long enough for the callback to register
        Thread.sleep(500);

        assertEquals(200, called.get());
    }

    @Test
    public void shouldOverrideAfterSuccess() throws Exception {
        final AtomicInteger called = new AtomicInteger(0);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().afterSuccess(b -> called.set(1)).create();
        assertEquals(2, gremlinExecutor.eval("1+1", null, new SimpleBindings(),
                GremlinExecutor.LifeCycle.build().afterSuccess(b -> called.set(200)).create()).get());

        // need to wait long enough for the callback to register
        Thread.sleep(500);

        assertEquals(200, called.get());
    }

    @Test
    public void shouldOverrideAfterFailure() throws Exception {
        final AtomicInteger called = new AtomicInteger(0);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().afterFailure((b,t) -> called.set(1)).create();
        try {
            gremlinExecutor.eval("10/0", null, new SimpleBindings(),
                    GremlinExecutor.LifeCycle.build().afterFailure((b,t) -> called.set(200)).create()).get();
            fail("Should have failed with division by zero");
        } catch (Exception ignored) {

        }

        // need to wait long enough for the callback to register
        Thread.sleep(500);

        assertEquals(200, called.get());
    }

    @Test
    public void shouldCallFail() throws Exception {
        final AtomicBoolean timeoutCalled = new AtomicBoolean(false);
        final AtomicBoolean successCalled = new AtomicBoolean(false);
        final AtomicBoolean failureCalled = new AtomicBoolean(false);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .afterFailure((b, e) -> failureCalled.set(true))
                .afterSuccess((b) -> successCalled.set(true))
                .afterTimeout((b) -> timeoutCalled.set(true)).create();
        try {
            gremlinExecutor.eval("10/0").get();
            fail();
        } catch (Exception ignored) { }

        // need to wait long enough for the callback to register
        Thread.sleep(500);

        assertFalse(timeoutCalled.get());
        assertFalse(successCalled.get());
        assertTrue(failureCalled.get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldCallSuccess() throws Exception {
        final AtomicBoolean timeoutCalled = new AtomicBoolean(false);
        final AtomicBoolean successCalled = new AtomicBoolean(false);
        final AtomicBoolean failureCalled = new AtomicBoolean(false);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .afterFailure((b, e) -> failureCalled.set(true))
                .afterSuccess((b) -> successCalled.set(true))
                .afterTimeout((b) -> timeoutCalled.set(true)).create();
        assertEquals(2, gremlinExecutor.eval("1+1").get());

        // need to wait long enough for the callback to register
        Thread.sleep(500);

        assertFalse(timeoutCalled.get());
        assertTrue(successCalled.get());
        assertFalse(failureCalled.get());
        gremlinExecutor.close();
    }

    @Test
    public void shouldCancelTimeoutOnSuccessfulEval() throws Exception {
        final long scriptEvaluationTimeout = 5_000;
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .scriptEvaluationTimeout(scriptEvaluationTimeout)
                .create();

        final long now = System.currentTimeMillis();
        assertEquals(2, gremlinExecutor.eval("1+1").get());
        gremlinExecutor.close();
        assertTrue(System.currentTimeMillis() - now < scriptEvaluationTimeout);
    }

    @Test
    public void shouldEvalInMultipleThreads() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicInteger i1 = new AtomicInteger(0);
        final AtomicBoolean b1 = new AtomicBoolean(false);
        final Thread t1 = new Thread(() -> {
            try {
                barrier.await();
                i1.set((Integer) gremlinExecutor.eval("1+1").get());
            } catch (Exception ex) {
                b1.set(true);
            }
        });

        final AtomicInteger i2 = new AtomicInteger(0);
        final AtomicBoolean b2 = new AtomicBoolean(false);
        final Thread t2 = new Thread(() -> {
            try {
                barrier.await();
                i2.set((Integer) gremlinExecutor.eval("1+1").get());
            } catch (Exception ex) {
                b2.set(true);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertEquals(2, i1.get());
        assertEquals(2, i2.get());
        assertFalse(b1.get());
        assertFalse(b2.get());

        gremlinExecutor.close();
    }

    @Test
    public void shouldNotExhaustThreads() throws Exception {
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2, testingThreadFactory);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .executorService(executorService)
                .scheduledExecutorService(executorService).create();

        final AtomicInteger count = new AtomicInteger(0);
        assertTrue(IntStream.range(0, 1000).mapToObj(i -> gremlinExecutor.eval("1+1")).allMatch(f -> {
            try {
                return (Integer) f.get() == 2;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                count.incrementAndGet();
            }
        }));

        assertEquals(1000, count.intValue());

        executorService.shutdown();
        executorService.awaitTermination(30000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shouldInitializeWithScript() throws Exception {
        final Map<String, Map<String,Object>> config = new HashMap<>();
        final Map<String, Object> scriptPluginConfig = new HashMap<>();
        scriptPluginConfig.put("files", Collections.singletonList(PATHS.get("GremlinExecutorInit.groovy")));
        config.put(ScriptFileGremlinPlugin.class.getName(), scriptPluginConfig);

        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addPlugins("gremlin-groovy", config)
                .create();

        assertEquals(2, gremlinExecutor.eval("add(1,1)").get());

        gremlinExecutor.close();
    }

    @Test
    public void shouldInitializeWithScriptAndMakeGlobalBinding() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addPlugins("gremlin-groovy", scriptFilePlugin)
                .create();

        assertEquals(2, gremlinExecutor.eval("add(1,1)").get());
        assertThat(gremlinExecutor.getScriptEngineManager().getBindings().keySet(), contains("name"));
        assertThat(gremlinExecutor.getScriptEngineManager().getBindings().keySet(), not(hasItem("someSet")));

        assertEquals("stephen", gremlinExecutor.getScriptEngineManager().getBindings().get("name"));

        gremlinExecutor.close();
    }

    @Test
    public void shouldContinueToEvalScriptsEvenWithTimedInterrupt() throws Exception {
        final Map<String, Map<String,Object>> config = new HashMap<>();
        final Map<String, Object> scriptPluginConfig = new HashMap<>();
        scriptPluginConfig.put("files", Collections.singletonList(PATHS.get("GremlinExecutorInit.groovy")));
        config.put(ScriptFileGremlinPlugin.class.getName(), scriptPluginConfig);

        final Map<String, Object> groovyPluginConfig = new HashMap<>();
        groovyPluginConfig.put("timedInterrupt", 250);
        config.put(GroovyCompilerGremlinPlugin.class.getName(), groovyPluginConfig);

        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addPlugins("gremlin-groovy", config)
                .create();

        for (int ix = 0; ix < 5; ix++) {
            try {
                // this script takes significantly longer than the interruptionTimeout
                gremlinExecutor.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}").get();
                fail("This should have timed out");
            } catch (Exception se) {
                assertEquals(TimedInterruptTimeoutException.class, se.getCause().getClass());
            }

            // this script takes significantly less than the interruptionTimeout
            assertEquals("test", gremlinExecutor.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 20) {};'test'").get());
        }

        gremlinExecutor.close();
    }

    @Test
    public void shouldInterruptWhile() throws Exception {
        final Map<String, Map<String,Object>> config = new HashMap<>();
        final Map<String, Object> scriptPluginConfig = new HashMap<>();
        scriptPluginConfig.put("files", Collections.singletonList(PATHS.get("GremlinExecutorInit.groovy")));
        config.put(ScriptFileGremlinPlugin.class.getName(), scriptPluginConfig);

        final Map<String, Object> groovyPluginConfig = new HashMap<>();
        groovyPluginConfig.put("enableThreadInterrupt", true);
        config.put(GroovyCompilerGremlinPlugin.class.getName(), groovyPluginConfig);

        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addPlugins("gremlin-groovy", config)
                .create();
        final AtomicBoolean asserted = new AtomicBoolean(false);

        final Thread t = new Thread(() -> {
            try {
                gremlinExecutor.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}").get();
            } catch (Exception se) {
                asserted.set(se instanceof InterruptedException);
            }
        });

        t.start();
        Thread.sleep(100);
        t.interrupt();
        while(t.isAlive()) {}

        assertTrue(asserted.get());
    }

    @Test
    public void shouldNotShutdownExecutorServicesSuppliedToGremlinExecutor() throws Exception {
        final ScheduledExecutorService service = Executors.newScheduledThreadPool(4, testingThreadFactory);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .executorService(service)
                .scheduledExecutorService(service).create();

        gremlinExecutor.close();
        assertFalse(service.isShutdown());
        service.shutdown();
        service.awaitTermination(30000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shouldGetExecutorService() throws Exception {
        final ScheduledExecutorService service = Executors.newScheduledThreadPool(4, testingThreadFactory);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .executorService(service)
                .scheduledExecutorService(service).create();

        assertSame(service, gremlinExecutor.getExecutorService());
        gremlinExecutor.close();
    }

    @Test
    public void shouldGetScheduledExecutorService() throws Exception {
        final ScheduledExecutorService service = Executors.newScheduledThreadPool(4, testingThreadFactory);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .executorService(service)
                .scheduledExecutorService(service).create();

        assertSame(service, gremlinExecutor.getScheduledExecutorService());
        gremlinExecutor.close();
    }

    @Test
    public void shouldAllowVariableReuseAcrossThreads() throws Exception {
        final ExecutorService service = Executors.newFixedThreadPool(8, testingThreadFactory);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();

        final AtomicBoolean failed = new AtomicBoolean(false);
        final int max = 512;
        final List<Pair<Integer, List<Integer>>> futures = Collections.synchronizedList(new ArrayList<>(max));
        IntStream.range(0, max).forEach(i -> {
            final int yValue = i * 2;
            final Bindings b = new SimpleBindings();
            b.put("x", i);
            b.put("y", yValue);
            final int zValue = i * -1;

            final String script = "z=" + zValue + ";[x,y,z]";
            try {
                service.submit(() -> {
                    try {
                        final List<Integer> result = (List<Integer>) gremlinExecutor.eval(script, b).get();
                        futures.add(Pair.with(i, result));
                    } catch (Exception ex) {
                        failed.set(true);
                    }
                });
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        service.shutdown();
        assertThat(service.awaitTermination(60000, TimeUnit.MILLISECONDS), is(true));

        // likely a concurrency exception if it occurs - and if it does then we've messed up because that's what this
        // test is partially designed to protected against.
        assertThat(failed.get(), is(false));

        assertEquals(max, futures.size());
        futures.forEach(t -> {
            assertEquals(t.getValue0(), t.getValue1().get(0));
            assertEquals(t.getValue0() * 2, t.getValue1().get(1).intValue());
            assertEquals(t.getValue0() * -1, t.getValue1().get(2).intValue());
        });
    }

    @Test
    public void shouldAllowConcurrentModificationOfGlobals() throws Exception {
        // this test simulates a scenario that likely shouldn't happen - where globals are modified by multiple
        // threads.  globals are created in a synchronized fashion typically but it's possible that someone
        // could do something like this and this test validate that concurrency exceptions don't occur as a
        // result
        final ExecutorService service = Executors.newFixedThreadPool(8, testingThreadFactory);
        final Bindings globals = new SimpleBindings();
        globals.put("g", -1);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addPlugins("gremlin-groovy", triggerPlugin)
                .globalBindings(globals).create();

        final AtomicBoolean failed = new AtomicBoolean(false);
        final int max = 512;
        final List<Pair<Integer, List<Integer>>> futures = Collections.synchronizedList(new ArrayList<>(max));
        IntStream.range(0, max).forEach(i -> {
            final int yValue = i * 2;
            final Bindings b = new SimpleBindings();
            b.put("x", i);
            b.put("y", yValue);
            final int zValue = i * -1;

            final String script = "z=" + zValue + ";[x,y,z,g]";
            try {
                service.submit(() -> {
                    try {
                        // modify the global in a separate thread
                        gremlinExecutor.getScriptEngineManager().put("g", i);
                        gremlinExecutor.getScriptEngineManager().put(Integer.toString(i), i);
                        gremlinExecutor.getScriptEngineManager().getBindings().keySet().stream().filter(s -> i % 2 == 0 && !s.equals("g")).findFirst().ifPresent(globals::remove);
                        final List<Integer> result = (List<Integer>) gremlinExecutor.eval(script, b).get();
                        futures.add(Pair.with(i, result));
                    } catch (Exception ex) {
                        failed.set(true);
                    }
                });
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        service.shutdown();
        assertThat(service.awaitTermination(60000, TimeUnit.MILLISECONDS), is(true));

        // likely a concurrency exception if it occurs - and if it does then we've messed up because that's what this
        // test is partially designed to protected against.
        assertThat(failed.get(), is(false));

        assertEquals(max, futures.size());
        futures.forEach(t -> {
            assertEquals(t.getValue0(), t.getValue1().get(0));
            assertEquals(t.getValue0() * 2, t.getValue1().get(1).intValue());
            assertEquals(t.getValue0() * -1, t.getValue1().get(2).intValue());
            assertThat(t.getValue1().get(3).intValue(), greaterThan(-1));
        });
    }
}
