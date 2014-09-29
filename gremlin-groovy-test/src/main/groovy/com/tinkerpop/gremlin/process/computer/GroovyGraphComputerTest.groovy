package com.tinkerpop.gremlin.process.computer

import com.tinkerpop.gremlin.process.computer.lambda.LambdaMapReduce
import com.tinkerpop.gremlin.process.computer.lambda.LambdaVertexProgram

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGraphComputerTest {

    public static class ComputerTest extends GraphComputerTest {
        public GraphComputer get_g_compute() {
            g.compute();
        }

        @Override
        public GraphComputer get_g_compute_setupXX_executeXX_terminateXtrueX_memoryKeysXset_incr_and_orX() {
            g.compute().program(LambdaVertexProgram.build().memoryComputeKeys("set", "incr", "and", "or").create());
        }

        @Override
        public GraphComputer get_g_compute_setupXX_executeXX_terminateXtrueX_memoryKeysXnullX() {
            g.compute().program(LambdaVertexProgram.build().memoryComputeKeys([null] as Set).create());
        }

        @Override
        public GraphComputer get_g_compute_setupXX_executeXX_terminateXtrueX_memoryKeysX_X() {
            g.compute().program(LambdaVertexProgram.build().memoryComputeKeys('').create());
        }

        @Override
        public GraphComputer get_g_compute_setupXsetXa_trueXX_executeXX_terminateXtrueX() {
            g.compute().program(LambdaVertexProgram.build().setup("gremlin-groovy", "a.set('a', true)").create());
        }

        @Override
        public GraphComputer get_g_compute_setupXX_executeXX_terminateXtrueX() {
            g.compute().program(LambdaVertexProgram.build().create());
        }

        @Override
        public GraphComputer get_g_compute_setupXX_executeXv_blah_m_incrX_terminateX1X_elementKeysXnameLengthCounterX_memoryKeysXa_bX() {
            return g.compute().program(LambdaVertexProgram.build().
                    execute("gremlin-groovy", """
                        import static org.junit.Assert.*;
                        try {
                            a.property("blah", "blah");
                            fail("Should throw an IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            assertEquals(GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey("blah").getMessage(), e.getMessage());
                        } catch (Exception e) {
                            fail("Should throw an IllegalArgumentException: " + e);
                        }

                        c.incr("a", 1);
                        if (c.isInitialIteration()) {
                            a.property("nameLengthCounter", a.<String>value("name").length());
                            c.incr("b", a.<String>value("name").length());
                        } else {
                            a.singleProperty("nameLengthCounter", a.<String>value("name").length() + a.<Integer>value("nameLengthCounter"));
                        }
                    """).terminate("gremlin-groovy", "a.getIteration() == 1")
                    .elementComputeKeys("nameLengthCounter").
                    memoryComputeKeys("a", "b").create());
        }

        @Override
        public GraphComputer get_g_compute_setupXabcdeX_executeXtestMemoryX_terminateXtestMemoryXmemoryKeysXabcdeX() {
            return g.compute().program(LambdaVertexProgram.build().
                    setup("gremlin-groovy", """
                        a.set("a", 0l);
                        a.set("b", 0l);
                        a.set("c", true);
                        a.set("d", false);
                        a.set("e", true);
                    """).
                    execute("gremlin-groovy", """
                        import static org.junit.Assert.*;
                        // test current step values
                        assertEquals(Long.valueOf(6 * c.getIteration()), c.get("a"));
                        assertEquals(Long.valueOf(0), c.get("b"));
                        if (c.isInitialIteration()) {
                            assertTrue(c.get("c"));
                            assertFalse(c.get("d"));
                        } else {
                            assertFalse(c.get("c"));
                            assertTrue(c.get("d"));
                        }
                        assertTrue(c.get("e"));

                        // update current step values and make sure returns are correct
                        assertEquals(Long.valueOf(6 * c.getIteration()) + 1l, c.incr("a", 1l));
                        assertEquals(Long.valueOf(0) + 1l, c.incr("b", 1l));
                        assertFalse(c.and("c", false));
                        assertTrue(c.or("d", true));
                        assertFalse(c.and("e", false));

                        // test current step values, should be the same as previous prior to update
                        assertEquals(Long.valueOf(6 * c.getIteration()), c.get("a"));
                        assertEquals(Long.valueOf(0), c.get("b"));
                        if (c.isInitialIteration()) {
                            assertTrue(c.get("c"));
                            assertFalse(c.get("d"));
                        } else {
                            assertFalse(c.get("c"));
                            assertTrue(c.get("d"));
                        }
                        assertTrue(c.get("e"));
                    """).
                    terminate("gremlin-groovy", """
                        import static org.junit.Assert.*;
                        assertEquals(Long.valueOf(6 * (a.getIteration() + 1)), a.get("a"));
                        assertEquals(Long.valueOf(6), a.get("b"));
                        assertFalse(a.get("c"));
                        assertTrue(a.get("d"));
                        assertFalse(a.get("e"));
                        a.set("b", 0l);
                        a.set("e", true);
                        return a.getIteration() > 1;
                    """).
                    memoryComputeKeys("a", "b", "c", "d", "e").create());
        }

        @Override
        public GraphComputer g_compute_mapXageXreduceXsumX_memoryXnextX_memoryKeyXageSumX() {
            g.compute().mapReduce(LambdaMapReduce.<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer> build()
                    .map("gremlin-groovy","if(a.property('age').isPresent()) b.emit(MapReduce.NullObject.instance(), a.value('age'))")
                    .reduce("gremlin-groovy", "c.emit(MapReduce.NullObject.instance(), b.sum())")
                    .memory("gremlin-groovy", "a.next().getValue1()")
                    .memoryKey("ageSum").create());
        }
    }
}
