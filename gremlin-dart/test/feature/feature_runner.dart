// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import 'dart:io';

class FeatureFile {
  final String path;
  final List<FeatureScenario> scenarios;

  const FeatureFile(this.path, this.scenarios);
}

class FeatureScenario {
  final String name;
  final Set<String> tags;
  final List<FeatureStep> steps;

  const FeatureScenario(this.name, this.tags, this.steps);
}

class FeatureStep {
  final String keyword;
  final String text;
  String? docString;
  List<Map<String, String>> table = <Map<String, String>>[];

  FeatureStep(this.keyword, this.text);
}

class FeatureRunner {
  FeatureFile parseFile(File file) =>
      FeatureFile(file.path, parse(file.readAsLinesSync()));

  List<FeatureScenario> parse(List<String> lines) {
    final scenarios = <FeatureScenario>[];
    final featureTags = <String>{};
    var pendingTags = <String>{};
    FeatureScenarioBuilder? currentScenario;
    FeatureStep? currentStep;
    var inDocString = false;
    final doc = StringBuffer();
    List<List<String>>? pendingTable;

    void flushTable() {
      if (currentStep == null || pendingTable == null || pendingTable!.isEmpty) {
        pendingTable = null;
        return;
      }
      final header = pendingTable!.first;
      currentStep!.table = pendingTable!.skip(1).map((row) {
        final values = <String, String>{};
        for (var i = 0; i < header.length && i < row.length; i++) {
          values[header[i]] = row[i];
        }
        return values;
      }).toList();
      pendingTable = null;
    }

    void flushScenario() {
      flushTable();
      if (currentScenario != null) scenarios.add(currentScenario!.build());
      currentScenario = null;
      currentStep = null;
    }

    for (final raw in lines) {
      final line = raw.trim();

      if (inDocString) {
        if (line == '"""') {
          currentStep?.docString = doc.toString().trim();
          doc.clear();
          inDocString = false;
        } else {
          doc.writeln(raw);
        }
        continue;
      }

      if (line.isEmpty || line.startsWith('#')) {
        continue;
      }
      if (line.startsWith('Feature:')) {
        featureTags.addAll(pendingTags);
        pendingTags = <String>{};
        continue;
      }
      if (line.startsWith('@')) {
        pendingTags.addAll(line
            .split(RegExp(r'\s+'))
            .where((tag) => tag.isNotEmpty)
            .map((tag) => tag.substring(1)));
        continue;
      }
      if (line.startsWith('Scenario:') || line.startsWith('Scenario Outline:')) {
        flushScenario();
        final name = line.substring(line.indexOf(':') + 1).trim();
        currentScenario = FeatureScenarioBuilder(name, {
          ...featureTags,
          ...pendingTags,
        });
        pendingTags = <String>{};
        continue;
      }
      if (line == '"""') {
        flushTable();
        inDocString = true;
        continue;
      }
      if (line.startsWith('|')) {
        pendingTable ??= <List<String>>[];
        pendingTable!.add(_parseTableRow(line));
        continue;
      }

      final step = _parseStep(line);
      if (step != null && currentScenario != null) {
        flushTable();
        currentScenario!.steps.add(step);
        currentStep = step;
      }
    }
    flushScenario();
    return scenarios;
  }

  FeatureStep? _parseStep(String line) {
    final match = RegExp(r'^(Given|When|Then|And|But)\s+(.+)$').firstMatch(line);
    if (match == null) return null;
    return FeatureStep(match.group(1)!, match.group(2)!);
  }

  List<String> _parseTableRow(String line) {
    final trimmed = line.trim();
    // Split on unescaped | (Gherkin escapes \| as a literal pipe)
    final inner = trimmed.substring(1, trimmed.length - 1);
    final cells = <String>[];
    final buf = StringBuffer();
    for (var i = 0; i < inner.length; i++) {
      if (inner[i] == '\\' && i + 1 < inner.length) {
        final next = inner[i + 1];
        if (next == '|') { buf.write('|'); i++; }
        else if (next == '\\') { buf.write('\\'); i++; }
        else if (next == 'n') { buf.write('\n'); i++; }
        else { buf.write(inner[i]); }
      } else if (inner[i] == '|') {
        cells.add(buf.toString().trim());
        buf.clear();
      } else {
        buf.write(inner[i]);
      }
    }
    cells.add(buf.toString().trim());
    return cells;
  }
}

class FeatureScenarioBuilder {
  final String name;
  final Set<String> tags;
  final List<FeatureStep> steps = <FeatureStep>[];

  FeatureScenarioBuilder(this.name, Set<String> tags) : tags = Set.of(tags);

  FeatureScenario build() => FeatureScenario(name, tags, List.of(steps));
}
