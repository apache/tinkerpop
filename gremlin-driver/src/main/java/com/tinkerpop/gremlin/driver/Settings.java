package com.tinkerpop.gremlin.driver;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Settings {

    public int port = 8182;

    public List<String> hosts = new ArrayList<>();

    public String serializer = "application/json";

    /**
     * Read configuration from a file into a new {@link Settings} object.
     *
     * @param stream an input stream containing a Gremlin Server YAML configuration
     * @return a new {@link java.util.Optional} object wrapping the created {@link Settings}
     */
    public static Settings read(final InputStream stream) {
        Objects.requireNonNull(stream);

        final Constructor constructor = new Constructor(Settings.class);
        final TypeDescription settingsDescription = new TypeDescription(Settings.class);
        settingsDescription.putListPropertyType("hosts", List.class);
        constructor.addTypeDescription(settingsDescription);

        final Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(stream, Settings.class);
    }
}
