package com.tinkerpop.gremlin.groovy.engine;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineTest;
import com.tinkerpop.gremlin.structure.Graph;
import org.junit.Ignore;
import org.junit.Test;
import org.kohsuke.groovy.sandbox.GroovyInterceptor;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutorTest {
    @Test
    public void shouldEvalScript() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        assertEquals(2, gremlinExecutor.eval("1+1").get());
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
    }

    @Test
    public void shouldEvalScriptWithBindings() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
        final Bindings b = new SimpleBindings();
        b.put("x", 1);
        assertEquals(2, gremlinExecutor.eval("1+x", b).get());
    }

    @Test
    public void shouldEvalScriptWithGlobalBindings() throws Exception {
        final Bindings b = new SimpleBindings();
        b.put("x", 1);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(b).create();
        assertEquals(2, gremlinExecutor.eval("1+x").get());
    }

    @Test
    public void shouldEvalScriptWithGlobalAndLocalBindings() throws Exception {
        final Bindings g = new SimpleBindings();
        g.put("x", 1);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(g).create();
        final Bindings b = new SimpleBindings();
        b.put("y", 1);
        assertEquals(2, gremlinExecutor.eval("y+x", b).get());
    }

    @Test
    public void shouldEvalScriptWithLocalOverridingGlobalBindings() throws Exception {
        final Bindings g = new SimpleBindings();
        g.put("x", 1);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(g).create();
        final Bindings b = new SimpleBindings();
        b.put("x", 10);
        assertEquals(11, gremlinExecutor.eval("x+1", b).get());
    }

    @Test
    public void shouldTimeoutScript() throws Exception {
        final AtomicBoolean timeoutCalled = new AtomicBoolean(false);
        final AtomicBoolean successCalled = new AtomicBoolean(false);
        final AtomicBoolean failureCalled = new AtomicBoolean(false);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .scriptEvaluationTimeout(500)
                .afterFailure((b, e) -> failureCalled.set(true))
                .afterSuccess((b) -> successCalled.set(true))
                .afterTimeout((b) -> timeoutCalled.set(true)).create();
        try {
            gremlinExecutor.eval("Thread.sleep(1000);10").get();
            fail();
        } catch (Exception ex) {

        }

        // need to wait long enough for the script to complete
        Thread.sleep(750);

        assertTrue(timeoutCalled.get());
        assertFalse(successCalled.get());
        assertFalse(failureCalled.get());
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
        } catch (Exception ex) {

        }

        // need to wait long enough for the script to complete
        Thread.sleep(750);

        assertFalse(timeoutCalled.get());
        assertFalse(successCalled.get());
        assertTrue(failureCalled.get());
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

        // need to wait long enough for the script to complete
        Thread.sleep(750);

        assertFalse(timeoutCalled.get());
        assertTrue(successCalled.get());
        assertFalse(failureCalled.get());
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
    }

    @Test
    public void shouldNotExhaustThreads() throws Exception {
        // this is not representative of how the GremlinExecutor should be configured.  A single thread executor
        // shared will create odd behaviors, but it's good for this test.
        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
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
    }

    @Test
    public void shouldFailUntilImportExecutes() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();

        final Set<String> imports = new HashSet<String>() {{
            add("import java.awt.Color");
        }};

        final AtomicInteger successes = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);

        // issue 1000 scripts in one thread using a class that isn't imported.  this will result in failure.
        // while that thread is running start a new thread that issues an addImports to include that class.
        // this should block further evals in the first thread until the import is complete at which point
        // evals in the first thread will resume and start to succeed
        final Thread t1 = new Thread(() ->
                IntStream.range(0, 1000).mapToObj(i -> gremlinExecutor.eval("Color.BLACK"))
                        .forEach(f -> {
                            f.exceptionally(t -> failures.incrementAndGet()).join();
                            if (!f.isCompletedExceptionally())
                                successes.incrementAndGet();
                        })
        );

        final Thread t2 = new Thread(() -> {
            while (failures.get() < 500) {
            }
            gremlinExecutor.getScriptEngines().addImports(imports);
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue(successes.intValue() > 0);
        assertTrue(failures.intValue() >= 500);
    }

    // todo: fix the below tests

    @Test
    @Ignore
    public void shouldInitializeWithScript() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addEngineSettings("gremlin-groovy",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList(new File(GremlinExecutorTest.class.getResource("GremlinExecutorInit.groovy").toURI()).getAbsolutePath()),
                        Collections.emptyMap())
                .create();

        assertEquals(2, gremlinExecutor.eval("sum(1,1)").get());
    }

    // todo: unignore the below test

    @Test
    @Ignore
    public void shouldSecureAll() throws Exception {
        GroovyInterceptor.getApplicableInterceptors().forEach(GroovyInterceptor::unregister);
        final Map<String,Object> config = new HashMap<>();
        config.put("sandbox", GremlinGroovyScriptEngineTest.DenyAll.class.getName());
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addEngineSettings("gremlin-groovy",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList(new File(GremlinExecutorTest.class.getResource("GremlinExecutorInit.groovy").toURI()).getAbsolutePath()),
                        config)
                .create();
        try {
            gremlinExecutor.eval("g = new TinkerGraph()").get();
            fail("Should have failed security");
        } catch (Exception se) {
            assertEquals(SecurityException.class, se.getCause().getCause().getCause().getCause().getClass());
        } finally {
            gremlinExecutor.close();
        }
    }

    // todo: get the below turned on

    @Test
    @Ignore
    public void shouldSecureSome() throws Exception {
        GroovyInterceptor.getApplicableInterceptors().forEach(GroovyInterceptor::unregister);
        final Map<String,Object> config = new HashMap<>();
        config.put("sandbox", GremlinGroovyScriptEngineTest.AllowSome.class.getName());
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addEngineSettings("gremlin-groovy",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList(new File(GremlinExecutorTest.class.getResource("GremlinExecutorInit.groovy").toURI()).getAbsolutePath()),
                        config)
                .create();
        try {
            gremlinExecutor.eval("g = 'new TinkerGraph()'").get();
            fail("Should have failed security");
        } catch (Exception se) {
            assertEquals(SecurityException.class, se.getCause().getCause().getCause().getCause().getClass());
        }

        try {
            final Graph g = (Graph) gremlinExecutor.eval("g = new TinkerGraph()").get();
            assertNotNull(g);
            // assertEquals(TinkerGraph.class, g.getClass()); // todo: how do we deal with this generically
        } catch (Exception ignored) {
            fail("Should not have tossed an exception");
        } finally {
            gremlinExecutor.close();
        }
    }

    // todo: fix the below tests

    @Test
    @Ignore
    public void shouldInitializeWithScriptAndWorkAfterRest() throws Exception {
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addEngineSettings("gremlin-groovy",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList(new File(GremlinExecutorTest.class.getResource("GremlinExecutorInit.groovy").toURI()).getAbsolutePath()),
                        Collections.emptyMap())
                .create();

        assertEquals(2, gremlinExecutor.eval("sum(1,1)").get());

        gremlinExecutor.getScriptEngines().reset();

        assertEquals(2, gremlinExecutor.eval("sum(1,1)").get());
    }
}
