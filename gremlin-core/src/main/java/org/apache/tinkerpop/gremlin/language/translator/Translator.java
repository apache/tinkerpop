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
package org.apache.tinkerpop.gremlin.language.translator;

import java.util.function.Function;

/**
 * Built in translator implementations to be provided
 */
public enum Translator {

    /**
     * Translates to gremlin-language.
     */
    LANGUAGE("Language", TranslateVisitor::new),

    /**
     * Translates to gremlin-language with anonymized arguments.
     */
    ANONYMIZED("Anonymized", AnonymizedTranslatorVisitor::new),

    /**
     * Translates to gremlin-dotnet.
     */
    DOTNET("DotNet", DotNetTranslateVisitor::new),

    /**
     * Translates to gremlin-groovy.
     */
    GO("Go", GoTranslateVisitor::new),

    /**
     * Translates to gremlin-groovy.
     */
    GROOVY("Groovy", GroovyTranslateVisitor::new),

    /**
     * Translates to gremlin-java.
     */
    JAVA("Java", JavaTranslateVisitor::new),

    /**
     * Translates to gremlin-javascript.
     */
    JAVASCRIPT("Javascript", JavascriptTranslateVisitor::new),

    /**
     * Translates to gremlin-python.
     */
    PYTHON("Python", PythonTranslateVisitor::new),

    ;

    private final String name;
    private final Function<String,TranslateVisitor> translateVisitorMaker;

    Translator(final String name, final Function<String,TranslateVisitor> translateVisitorMaker) {
        this.name = name;
        this.translateVisitorMaker = translateVisitorMaker;
    }

    public String getName() {
        return name;
    }

    public TranslateVisitor getTranslateVisitor(final String graphTraversalSourceName) {
        return translateVisitorMaker.apply(graphTraversalSourceName);
    }

    @Override
    public String toString() {
        return "Translator[" + name + "]";
    }
}
