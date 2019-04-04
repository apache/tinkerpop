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
package org.apache.tinkerpop.machine;

import org.apache.tinkerpop.language.gremlin.common.__;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.compiler.Order;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.tinkerpop.language.gremlin.P.gt;
import static org.apache.tinkerpop.language.gremlin.P.lt;
import static org.apache.tinkerpop.language.gremlin.common.__.choose;
import static org.apache.tinkerpop.language.gremlin.common.__.incr;
import static org.apache.tinkerpop.language.gremlin.common.__.is;
import static org.apache.tinkerpop.language.gremlin.common.__.union;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleTestSuite extends AbstractTestSuite<Long> {

    public SimpleTestSuite(final Machine machine, final Bytecode<Long> source) {
        super(machine, source);
    }

    // LINEAR TESTING

    @Test
    void g_injectX1_2_3X_incr() {
        verify(List.of(2L, 3L, 4L),
                g.inject(1L, 2L, 3L).incr());
    }

    @Test
    void g_injectX1_2_3X_incr_sum() {
        verify(List.of(9L),
                g.inject(1L, 2L, 3L).incr().sum());
    }

    @Test
    void g_injectX1_2_3X_isXgtX1XX_incr() {
        verify(List.of(3L, 4L),
                g.inject(1L, 2L, 3L).is(gt(1)).incr());
    }

    // NESTED TESTING

    @Test
    void g_injectX1_2_3X_mapXincrX() {
        verify(List.of(2L, 3L, 4L),
                g.inject(1L, 2L, 3L).map(incr()));
    }

    @Test
    void g_injectX1_2_3X_chooseXisXgtX1XX__incrX() {
        verify(List.of(3L, 4L),
                g.inject(1L, 2L, 3L).choose(is(gt(1L)), incr()));
    }

    @Test
    void g_injectX1_2_3X_chooseXisXgtX1XX__incr__incr_incrX() {
        verify(List.of(3L, 3L, 4L),
                g.inject(1L, 2L, 3L).choose(is(gt(1L)), incr(), __.<Long>incr().incr()));
    }

    @Test
    void g_injectX1_2_3X_unionXincr__incr_incrX() {
        verify(List.of(2L, 3L, 4L, 3L, 4L, 5L),
                g.inject(1L, 2L, 3L).union(incr(), __.<Long>incr().incr()));
    }

    @Test
    void g_injectX1_2_3X_unionXincr__incr_incr__incr_incr_incrX() {
        verify(List.of(2L, 3L, 4L, 3L, 4L, 5L, 4L, 5L, 6L),
                g.inject(1L, 2L, 3L).union(incr(), __.<Long>incr().incr(), __.<Long>incr().incr().incr()));
    }

    @Test
    void g_injectX1_2_3X_unionXincr__unionXincr_incr__incr_incr_incrXX() {
        verify(List.of(2L, 3L, 4L, 3L, 4L, 5L, 4L, 5L, 6L),
                g.inject(1L, 2L, 3L).union(incr(), union(__.<Long>incr().incr(), __.<Long>incr().incr().incr())));
    }

    // REPEAT EMIT/UNTIL TESTING

    @Test
    void g_injectX1X_repeatXincrX_untilXisX4XX() { // r.u
        verify(List.of(4L),
                g.inject(1L).repeat(incr()).until(is(4L)));
    }

    @Test
    void g_injectX1X_untilXisX4XX_repeatXincrX() { // u.r
        verify(List.of(4L),
                g.inject(1L).until(is(4L)).repeat(incr()));
    }

    @Test
    void g_injectX1X_emit_repeatXincrX_untilXisX4XX() { // e.r.u
        verify(List.of(1L, 2L, 3L, 4L),
                g.inject(1L).emit().repeat(incr()).until(is(4L)));
    }

    @Test
    void g_injectX1X_emit_untilXisX4XX_repeatXincrX() { // e.u.r
        verify(List.of(1L, 2L, 3L, 4L, 4L),
                g.inject(1L).emit().until(is(4L)).repeat(incr()));
    }

    @Test
    void g_injectX1X_untilXisX4XX_emit_repeatXincrX() { // u.e.r
        verify(List.of(1L, 2L, 3L, 4L),
                g.inject(1L).until(is(4L)).emit().repeat(incr()));
    }

    @Test
    void g_injectX1X_untilXisX4XX_repeatXincrX_emit() { // u.r.e
        verify(List.of(2L, 3L, 4L, 4L),
                g.inject(1L).until(is(4L)).repeat(incr()).emit());
    }

    @Test
    void g_injectX1X_repeatXincrX_emit_untilXisX4XX() { // r.e.u
        verify(List.of(2L, 3L, 4L, 4L),
                g.inject(1L).repeat(incr()).emit().until(is(4L)));
    }

    @Test
    void g_injectX1X_repeatXincrX_untilXisX4XX_emit() { // r.u.e
        verify(List.of(2L, 3L, 4L),
                g.inject(1L).repeat(incr()).until(is(4L)).emit());
    }

    // REPEAT TIMES TESTING

    @Test
    void g_injectX1_2_3X_repeatXincrX_timesX1X() { // r.t
        verify(List.of(2L, 3L, 4L),
                g.inject(1L, 2L, 3L).repeat(incr()).times(1));
    }

    @Test
    void g_injectX1_2_3X_repeatXincrX_timesX2X() { // r.t
        verify(List.of(3L, 4L, 5L),
                g.inject(1L, 2L, 3L).repeat(incr()).times(2));
    }

    @Test
    void g_injectX1_2_3X_repeatXincrX_timesX3X() { // r.t
        verify(List.of(4L, 5L, 6L),
                g.inject(1L, 2L, 3L).repeat(incr()).times(3));
    }

    @Test
    void g_injectX1_2_3X_repeatXincr_incrX_timesX3X() { // r.t
        verify(List.of(7L, 8L, 9L),
                g.inject(1L, 2L, 3L).repeat(__.<Long>incr().incr()).times(3));
    }

    @Test
    void g_injectX1_2_3X_timesX0X_repeatXincrX() { // t.r
        verify(List.of(1L, 2L, 3L),
                g.inject(1L, 2L, 3L).times(0).repeat(incr()));
    }

    @Test
    void g_injectX1_2_3X_timesX1X_repeatXincrX() { // t.r
        verify(List.of(2L, 3L, 4L),
                g.inject(1L, 2L, 3L).times(1).repeat(incr()));
    }

    @Test
    void g_injectX1_2_3X_timesX2X_repeatXincrX() { // t.r
        verify(List.of(3L, 4L, 5L),
                g.inject(1L, 2L, 3L).times(2).repeat(incr()));
    }

    @Test
    void g_injectX1_2_3X_timesX3X_repeatXincrX() { // t.r
        verify(List.of(4L, 5L, 6L),
                g.inject(1L, 2L, 3L).times(3).repeat(incr()));
    }

    // NESTED REPEAT TESTING

    @Test
    void g_injectX1_2_3X_repeatXincr_mapXchooseXisXgtX5XX__incr__incr_incrXXX_timesX3X() {
        verify(List.of(9L, 9L, 10L),
                g.inject(1L, 2L, 3L).repeat(__.<Long>incr().map(choose(is(gt(5L)), incr(), __.<Long>incr().incr()))).times(3));
    }

    @Test
    void g_injectX1_2_3X_repeatXincr_flatMapXchooseXisXltX8XX__incrXXX_timesX3X() {
        verify(List.of(7L, 8L),
                g.inject(1L, 2L, 3L).repeat(__.<Long>incr().flatMap(choose(is(lt(8L)), incr()))).times(3));
    }

    //@Test
    void g_injectX1X_repeatXunionXincr__incr_incrXX_timesX1X() {
        verify(List.of(2L, 3L),
                g.inject(1L).repeat(union(incr(), __.<Long>incr().incr())).times(1));
    }

    //@Test
    void g_injectX1X_repeatXunionXincr__incr_incrXX_timesX2X() {
        verify(List.of(3L, 4L, 4L, 5L),
                g.inject(1L).repeat(union(incr(), __.<Long>incr().incr())).times(2));
    }

    //@Test
    void g_injectX1X_repeatXunionXincr__incr_incrXX_timesX3X() {
        verify(List.of(4L, 5L, 5L, 6L, 5L, 6L, 6L, 7L),
                g.inject(1L).repeat(union(incr(), __.<Long>incr().incr())).times(3));
    }

    // UNFOLD TESTING

    @Test
    void g_injectXlistX1_2_3XX_unfold_incr() {
        verifyOrder(List.of(2L, 3L, 4L),
                g.inject(List.of(1L, 2L, 3L)).unfold().incr());
    }


    // ORDER TESTING

    @Test
    void g_injectX7_3_5_20_1_2_5X_incr_order_byXdescX() {
        verifyOrder(List.of(21L, 8L, 6L, 6L, 4L, 3L, 2L),
                g.inject(7L, 3L, 5L, 20L, 1L, 2L, 5L).incr().order().by(Order.desc));
    }

}
