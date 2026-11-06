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
package org.apache.tinkerpop.tinkeradoc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.jruby.extension.spi.ExtensionRegistry;

/**
 * SPI entry point that registers the TinkerPop documentation extensions with AsciidoctorJ.
 * Registers both the Treeprocessor (for gremlin block processing) and the Postprocessor
 * (for callout fixes, version replacement, etc.)
 */
public class GremlinDocsExtension implements ExtensionRegistry {

    @Override
    public void register(final Asciidoctor asciidoctor) {
        asciidoctor.javaExtensionRegistry()
                .treeprocessor(GremlinTreeprocessor.class)
                .postprocessor(GremlinPostprocessor.class);
    }
}
