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

import 'package:test/test.dart';

import 'cucumber_world.dart';
import 'feature_runner.dart';
import 'graph_setup.dart';
import 'steps.dart';

const defaultFeatureDir =
    '../../gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/test/features';
const defaultServerUrl = 'http://localhost:45940/gremlin';

void main() {
  final featureDir =
      Platform.environment['CUCUMBER_FEATURE_FOLDER'] ?? defaultFeatureDir;
  final serverUrl =
      Platform.environment['GREMLIN_SERVER_URL'] ?? defaultServerUrl;
  final featureFiles = _discoverFeatureFiles(featureDir);
  final runner = FeatureRunner();

  late Map<String, DataGraph> graphDataMap;
  Object? setupError;

  setUpAll(() async {
    try {
      await GraphSetup(serverUrl).submit('g.inject(1)', 'ggraph');
      graphDataMap = await GraphSetup(serverUrl).loadAllDataGraphs();
    } catch (error) {
      setupError = error;
      print('Skipping gremlin-dart feature tests: Gremlin Server at '
          '$serverUrl is unreachable or not ready ($error)');
    }
  });

  group('Gremlin language feature tests', () {
    for (final file in featureFiles) {
      final feature = runner.parseFile(file);
      for (final scenario in feature.scenarios) {
        test('${_relativePath(file.path)}: ${scenario.name}', () async {
          if (setupError != null) {
            markTestSkipped('Gremlin Server at $serverUrl is unreachable.');
            return;
          }
          final world = CucumberWorld(serverUrl, graphDataMap);
          await FeatureSteps(world).run(scenario);
        });
      }
    }

    if (featureFiles.isEmpty) {
      test('feature discovery', () {
        if (setupError != null) {
          markTestSkipped('Gremlin Server at $serverUrl is unreachable.');
          return;
        }
        fail('No .feature files found under $featureDir');
      });
    }
  });
}

List<File> _discoverFeatureFiles(String featureDir) {
  final dir = _featureDirectory(featureDir);
  if (!dir.existsSync()) return <File>[];
  final files = dir
      .listSync(recursive: true)
      .whereType<File>()
      .where((file) => file.path.endsWith('.feature'))
      .toList();
  files.sort((a, b) => a.path.compareTo(b.path));
  return files;
}

Directory _featureDirectory(String featureDir) {
  final dir = Directory(featureDir);
  if (dir.existsSync() || featureDir.startsWith('/')) return dir;

  final monorepoRelative = Directory('../$featureDir');
  if (monorepoRelative.existsSync()) return monorepoRelative;

  final packageRelative = Directory(featureDir.replaceFirst('../../', '../'));
  if (packageRelative.existsSync()) return packageRelative;

  return dir;
}

String _relativePath(String path) {
  final cwd = Directory.current.absolute.path;
  final absolute = File(path).absolute.path;
  if (absolute.startsWith(cwd)) {
    return absolute.substring(cwd.length + 1);
  }
  return path;
}
