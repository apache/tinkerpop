package com.tinkerpop.gremlin.groovy.engine;

import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutorTest {
	@Test
	public void shouldEvalScript() throws Exception {
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create().build();
		assertEquals(2, gremlinExecutor.eval("1+1").get());
	}

	@Test
	public void shouldEvalMultipleScripts() throws Exception {
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create().build();
		assertEquals(2, gremlinExecutor.eval("1+1").get());
		assertEquals(3, gremlinExecutor.eval("1+2").get());
		assertEquals(4, gremlinExecutor.eval("1+3").get());
		assertEquals(5, gremlinExecutor.eval("1+4").get());
		assertEquals(6, gremlinExecutor.eval("1+5").get());
		assertEquals(7, gremlinExecutor.eval("1+6").get());
	}

	@Test
	public void shouldEvalScriptWithBindings() throws Exception {
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create().build();
		final Bindings b = new SimpleBindings();
		b.put("x", 1);
		assertEquals(2, gremlinExecutor.eval("1+x", b).get());
	}

	@Test
	public void shouldEvalScriptWithGlobalBindings() throws Exception {
		final Bindings b = new SimpleBindings();
		b.put("x", 1);
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create().globalBindings(b).build();
		assertEquals(2, gremlinExecutor.eval("1+x").get());
	}

	@Test
	public void shouldEvalScriptWithGlobalAndLocalBindings() throws Exception {
		final Bindings g = new SimpleBindings();
		g.put("x", 1);
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create().globalBindings(g).build();
		final Bindings b = new SimpleBindings();
		b.put("y", 1);
		assertEquals(2, gremlinExecutor.eval("y+x", b).get());
	}

	@Test
	public void shouldEvalScriptWithLocalOverridingGlobalBindings() throws Exception {
		final Bindings g = new SimpleBindings();
		g.put("x", 1);
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create().globalBindings(g).build();
		final Bindings b = new SimpleBindings();
		b.put("x", 10);
		assertEquals(11, gremlinExecutor.eval("x+1", b).get());
	}

	@Test
	public void shouldTimeoutScript() throws Exception {
		final AtomicBoolean timeoutCalled = new AtomicBoolean(false);
		final AtomicBoolean successCalled = new AtomicBoolean(false);
		final AtomicBoolean failureCalled = new AtomicBoolean(false);
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create()
				.scriptEvaluationTimeout(500)
				.afterFailure((b,e) -> failureCalled.set(true))
				.afterSuccess((b) -> successCalled.set(true))
				.afterTimeout((b) -> timeoutCalled.set(true)).build();
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
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create()
				.afterFailure((b,e) -> failureCalled.set(true))
				.afterSuccess((b) -> successCalled.set(true))
				.afterTimeout((b) -> timeoutCalled.set(true)).build();
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
		final GremlinExecutor gremlinExecutor = GremlinExecutor.create()
				.afterFailure((b,e) -> failureCalled.set(true))
				.afterSuccess((b) -> successCalled.set(true))
				.afterTimeout((b) -> timeoutCalled.set(true)).build();
		assertEquals(2, gremlinExecutor.eval("1+1").get());

		// need to wait long enough for the script to complete
		Thread.sleep(750);

		assertFalse(timeoutCalled.get());
		assertTrue(successCalled.get());
		assertFalse(failureCalled.get());
	}
}
