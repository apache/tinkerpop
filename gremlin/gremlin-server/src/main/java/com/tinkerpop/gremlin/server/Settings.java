package com.tinkerpop.gremlin.server;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Server settings as configured by a YAML file.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Settings {

    public String host;
    public int port;
    public int threadPoolWorker;
    public int threadPoolBoss;
    public ServerMetrics metrics = null;
    public Map<String, String> graphs;
    public Map<String, ScriptEngineSettings> scriptEngines;
    public String staticFilePath;
    public List<List<String>> use;

    public Optional<ServerMetrics> optionalMetrics() {
        return Optional.ofNullable(metrics);
    }

    public static Optional<Settings> read(final String file) {
        try {
            final InputStream input = new FileInputStream(new File(file));
            return read(input);
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    public static Optional<Settings> read(final InputStream stream) {
        if (stream == null)
            return Optional.empty();

        try {
            final Constructor constructor = new Constructor(Settings.class);
            final TypeDescription settingsDescription = new TypeDescription(Settings.class);
            settingsDescription.putMapPropertyType("graphs", String.class, String.class);
            settingsDescription.putMapPropertyType("scriptEngines", String.class, ScriptEngineSettings.class);
            settingsDescription.putListPropertyType("use", List.class);
            constructor.addTypeDescription(settingsDescription);

            final TypeDescription scriptEngineSettingsDescription = new TypeDescription(ScriptEngineSettings.class);
            scriptEngineSettingsDescription.putListPropertyType("imports", String.class);
            scriptEngineSettingsDescription.putListPropertyType("staticImports", String.class);
            constructor.addTypeDescription(scriptEngineSettingsDescription);

            final TypeDescription serverMetricsDescription = new TypeDescription(ServerMetrics.class);
            constructor.addTypeDescription(serverMetricsDescription);

            final TypeDescription consoleReporterDescription = new TypeDescription(ConsoleReporterMetrics.class);
            constructor.addTypeDescription(consoleReporterDescription);

            final TypeDescription csvReporterDescription = new TypeDescription(CsvReporterMetrics.class);
            constructor.addTypeDescription(csvReporterDescription);

            final Yaml yaml = new Yaml(constructor);
            return Optional.of(yaml.loadAs(stream, Settings.class));
        } catch (Exception fnfe) {
            return Optional.empty();
        }
    }

    public static class ScriptEngineSettings {
        public List<String> imports;
        public List<String> staticImports;
    }

    public static class ServerMetrics {
        public ConsoleReporterMetrics consoleReporter = null;
        public CsvReporterMetrics csvReporter = null;

        public Optional<ConsoleReporterMetrics> optionalConsoleReporter() {
            return Optional.ofNullable(consoleReporter);
        }

        public Optional<CsvReporterMetrics> optionalCsvReporter() {
            return Optional.ofNullable(csvReporter);
        }
    }

    public static class ConsoleReporterMetrics extends IntervalBasedMetrics {
    }

    public static class CsvReporterMetrics extends IntervalBasedMetrics {
        public String fileName = "metrics.csv";
    }

    public static abstract class IntervalBasedMetrics {
        public long interval = 60000;
    }
}
