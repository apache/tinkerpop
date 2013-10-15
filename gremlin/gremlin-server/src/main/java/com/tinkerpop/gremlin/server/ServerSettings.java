package com.tinkerpop.gremlin.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerSettings {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);

    public String host;
    public int port;
    public Map<String, String> graphs;

    public static Optional<ServerSettings> read(final String file) {
        try {
            final InputStream input = new FileInputStream(new File(file));

            final Constructor constructor = new Constructor(ServerSettings.class);
            final TypeDescription settingsDescription = new TypeDescription(ServerSettings.class);
            settingsDescription.putMapPropertyType("graphs", String.class, String.class);
            constructor.addTypeDescription(settingsDescription);

            final Yaml yaml = new Yaml(constructor);
            return Optional.of(yaml.loadAs(input, ServerSettings.class));
        } catch (FileNotFoundException fnfe) {
            return Optional.empty();
        }
    }
}
