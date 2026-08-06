/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * The {@link IO#gryo} branches of {@link IoStep} build their own hardened {@link GryoMapper}, and nothing else in the
 * suite covers them - they could be deleted and the build would stay green. An end-to-end {@code read()} cannot fill
 * the gap either: {@code readGraph} rejects crafted bytes on the header check before the mapper is consulted, and a
 * legitimate graph document never carries one of the dropped types. So this asserts the wiring instead, against the
 * {@code Kryo} the step actually hands to its reader and writer.
 * <p/>
 * It lives in this package rather than beside {@link IoStep} because {@code getKryo()} is package private here.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoIoStepHardeningTest {

    @Test
    public void shouldBuildHardenedGryoReaderAndWriter() {
        final ExposedIoStep step = new ExposedIoStep("graph.kryo");

        for (final Kryo kryo : Arrays.asList(((GryoReader) step.reader()).getKryo(),
                ((GryoWriter) step.writer()).getKryo())) {
            try {
                kryo.getRegistration(OptionsStrategy.class);
                fail("io() must not register a JavaSerializer backed type such as OptionsStrategy");
            } catch (IllegalArgumentException expected) {
                // Kryo refuses an unregistered class while registration is required, which is the whole point of
                // dropping the registration rather than replacing its serializer
            }
        }
    }

    /**
     * {@code constructReader()} and {@code constructWriter()} are protected, which only grants access through a
     * reference of the subclass's own type, so the calls are made from inside the subclass rather than on an
     * {@code IoStep} typed variable.
     */
    private static final class ExposedIoStep extends IoStep<Object> {

        ExposedIoStep(final String file) {
            // the .kryo extension is what drives detectFileType() to IO.gryo, and neither construct method reads the
            // graph off the traversal, so an anonymous start is enough to hold the step
            super(__.start().asAdmin(), file);
        }

        GraphReader reader() {
            return constructReader();
        }

        GraphWriter writer() {
            return constructWriter();
        }
    }
}
