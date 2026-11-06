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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.script;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapClassLoadingPolicy;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ScriptRecordReaderTest {

    @Test
    public void shouldRefuseToRunScriptInUntrustedMode() throws Exception {
        final Configuration configuration = new Configuration(false);
        configuration.set(ScriptRecordReader.SCRIPT_FILE, "irrelevant.groovy");
        // default (untrusted) -- the trust check runs before the split/filesystem is touched, so a null split is fine
        final TaskAttemptContext context = new TaskAttemptContextImpl(configuration, new TaskAttemptID());
        try {
            new ScriptRecordReader().initialize(null, context);
            fail("ScriptRecordReader must refuse to compile/run a script in untrusted mode");
        } catch (final IllegalStateException ise) {
            assertTrue(ise.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldRunScriptWhenFormatApprovedInUntrustedMode() throws Exception {
        final Configuration configuration = new Configuration(false);
        configuration.set(ScriptRecordReader.SCRIPT_FILE, "irrelevant.groovy");
        // untrusted, but the operator explicitly approved ScriptInputFormat -> the trust gate must pass (init then
        // fails later on the null split, not on the gate)
        configuration.set(OlapClassLoadingPolicy.APPROVED_CLASSES, ScriptInputFormat.class.getName());
        final TaskAttemptContext context = new TaskAttemptContextImpl(configuration, new TaskAttemptID());
        try {
            new ScriptRecordReader().initialize(null, context);
        } catch (final Exception e) {
            // a downstream failure on the null split is fine; only the trust-gate rejection (which names the trust
            // flag) means the approved-class opt-in did not take effect
            assertFalse("an approved ScriptInputFormat must pass the trust gate, but was rejected: " + e.getMessage(),
                    String.valueOf(e.getMessage()).contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldRejectScriptConfiguredAsGraphReaderButNotApprovedInUntrustedMode() throws Exception {
        final Configuration configuration = new Configuration(false);
        configuration.set(ScriptRecordReader.SCRIPT_FILE, "irrelevant.groovy");
        // the gate seeds ONLY from gremlin.io.approvedClasses, never from the configured graphReader -- otherwise
        // naming ScriptInputFormat as the reader would self-approve it. Configure it as the reader but do NOT approve
        // it; the gate must still reject.
        configuration.set(Constants.GREMLIN_HADOOP_GRAPH_READER, ScriptInputFormat.class.getName());
        final TaskAttemptContext context = new TaskAttemptContextImpl(configuration, new TaskAttemptID());
        try {
            new ScriptRecordReader().initialize(null, context);
            fail("ScriptInputFormat configured as the graphReader must not self-approve; the gate must still reject");
        } catch (final IllegalStateException ise) {
            assertTrue(ise.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }
}
