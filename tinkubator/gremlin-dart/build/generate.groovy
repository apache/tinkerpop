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

import org.apache.tinkerpop.gremlin.language.corpus.FeatureReader
import org.apache.tinkerpop.gremlin.language.translator.DartTranslateVisitor
import org.apache.tinkerpop.gremlin.language.translator.GremlinTranslator

import java.nio.file.Files
import java.nio.file.Paths

/** Escapes a string for embedding as a single-quoted Dart string literal. */
static String dartLiteral(String value) {
    return "'" + value.replace('\\', '\\\\').replace("'", "\\'").replace('\$', '\\\$') + "'"
}
import java.nio.file.StandardCopyOption

final File dartGremlinFile = new File("${projectBaseDir}/tinkubator/gremlin-dart/test/feature/gremlin.dart")
final def featureRoot = Paths.get("${projectBaseDir}", "gremlin-test", "src", "main", "resources", "org", "apache", "tinkerpop", "gremlin", "test", "features")
final Map<String, List<String>> gremlins = new LinkedHashMap<String, List<String>>()
Files.find(featureRoot, Integer.MAX_VALUE, { path, attributes ->
    attributes.isRegularFile() && path.toString().endsWith('.feature')
}).withCloseable { paths ->
    paths.sorted().forEach { featureFile ->
        final String relativePath = featureRoot.relativize(featureFile).toString().replace(File.separator, '/')
        FeatureReader.parseGrouped(featureFile.toString()).each { String scenarioName, List<String> scripts ->
            final String scenarioKey = "${relativePath}::${scenarioName}"
            if (gremlins.put(scenarioKey, scripts) != null) {
                throw new IllegalStateException("Duplicate scenario key: ${scenarioKey}")
            }
        }
    }
}
final def temporaryDartGremlinFile = Files.createTempFile(dartGremlinFile.parentFile.toPath(), 'gremlin-', '.dart')

try {
temporaryDartGremlinFile.toFile().withWriter('UTF-8') { Writer writer ->
    writer.writeLine('// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.\n' +
            '// See the NOTICE file distributed with this work for additional information regarding copyright ownership.\n' +
            '// The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use\n' +
            '// this file except in compliance with the License.  You may obtain a copy of the License at\n' +
            '//\n' +
            '//   http://www.apache.org/licenses/LICENSE-2.0\n' +
            '//\n' +
            '// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed\n' +
            '// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License\n' +
            '// for the specific language governing permissions and limitations under the License.\n' +
            '// ignore_for_file: non_constant_identifier_names\n' +
            '// AUTO-GENERATED - do not edit. Run build/generate.groovy to regenerate.\n')
    writer.writeLine("import 'dart:convert';")
    writer.writeLine("import 'package:gremlin_dart/process/anonymous_traversal.dart';")
    writer.writeLine("import 'package:gremlin_dart/process/graph_traversal.dart';")
    writer.writeLine("import 'package:gremlin_dart/process/traversal.dart';")
    writer.writeLine("import 'package:gremlin_dart/process/traversal_strategy.dart';\n")
    writer.writeLine("import 'package:uuid/uuid.dart';\n")

    // Each entry is a function of (GraphTraversalSource g, {named parameters}). Parameters are the variables
    // a scenario declares with "using the parameter"; the feature runner supplies them by name.
    writer.writeLine('\nfinal Map<String, List<Function>> generatedTraversals = <String, List<Function>>{')
    final Map<String, Set<String>> generatedParameters = new LinkedHashMap<String, Set<String>>()
    gremlins.each { String scenarioKey, List<String> scripts ->
        try {
            final Set<String> parameters = new LinkedHashSet<String>()
            final List<String> translatedScripts = scripts.collect { String script ->
                final def translation = GremlinTranslator.translate(script, new DartTranslateVisitor())
                final String translated = translation.getTranslated()
                parameters.addAll(translation.getParameters())
                return translated
            }

            // Every function in a scenario declares the same named parameters so the runner can pass them all.
            final String signature = parameters.isEmpty() ? '' :
                    ', {' + parameters.collect { "dynamic ${it}" }.join(', ') + '}'
            writer.writeLine("  ${dartLiteral(scenarioKey)}: <Function>[")
            translatedScripts.each { String translated ->
                writer.writeLine("    (GraphTraversalSource g${signature}) => " + translated + ',')
            }
            writer.writeLine('  ],')
            generatedParameters.put(scenarioKey, parameters)
        } catch (Exception error) {
            throw new IllegalStateException("Cannot translate feature scenario: ${scenarioKey}", error)
        }
    }

    writer.writeLine('};')
    writer.writeLine('\nfinal Map<String, Set<String>> generatedTraversalParameters = <String, Set<String>>{')
    generatedParameters.each { String scenarioKey, Set<String> parameters ->
        final String names = parameters.collect { "'${it}'" }.join(', ')
        writer.writeLine("  ${dartLiteral(scenarioKey)}: <String>{${names}},")
    }
    writer.writeLine('};')

    println "gremlin-dart: generated ${generatedParameters.size()} grouped scenarios"
}
    Files.move(temporaryDartGremlinFile, dartGremlinFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
} finally {
    Files.deleteIfExists(temporaryDartGremlinFile)
}
