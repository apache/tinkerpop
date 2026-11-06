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
package org.apache.tinkerpop.gremlin.fluency;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PromptTest {

    @Test
    public void shouldCarryIntentButNotGraphOrScenarioName() {
        final IntentScenario scenario = CorpusTest.find("g_VX4X_bothE_otherV");
        final String prompt = Prompt.render(scenario);
        assertThat(prompt, containsString(scenario.getIntent()));
        assertThat(prompt, not(containsString(scenario.getGraph())));
        assertThat(prompt, not(containsString(scenario.getName())));
        assertThat(prompt, not(containsString(scenario.getTraversal())));
    }

    @Test
    public void shouldDropTemplateComments() {
        final String prompt = Prompt.render(CorpusTest.find("g_VX4X_bothE_otherV"));
        assertThat(prompt, not(containsString("Licensed")));
        assertThat(prompt.startsWith("Write one Gremlin traversal"), is(true));
    }

    @Test
    public void shouldHashTemplate() {
        assertThat(Prompt.templateHash().length(), is(12));
        assertThat(Prompt.templateHash(), is(Prompt.templateHash()));
    }
}
