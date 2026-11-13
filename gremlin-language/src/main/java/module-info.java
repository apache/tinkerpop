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

/**
 * The ANTLR-generated Gremlin parser. The {@code org.apache.tinkerpop.gremlin.language.corpus} package is build and
 * test support for the grammar and is not exported.
 */
module org.apache.tinkerpop.gremlin.language {
    // the generated parser, lexer and visitors extend ANTLR runtime types, which only has an Automatic-Module-Name
    requires transitive org.antlr.antlr4.runtime;

    // used only by the unexported corpus package
    requires javatuples;
    requires org.apache.commons.text;

    exports org.apache.tinkerpop.gremlin.language.antlr;
}
