package com.tinkerpop.gremlin.util.config;

import org.apache.commons.configuration.AbstractHierarchicalFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Commons Configuration object for YAML.  Adapted from code originally found here:
 * https://github.com/PEXPlugins/SimpleConfigs
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class YamlConfiguration extends AbstractHierarchicalFileConfiguration {
    public final static int DEFAULT_IDENT = 4;
    private final DumperOptions yamlOptions = new DumperOptions();
    private final Yaml yaml = new Yaml(yamlOptions);
    private boolean xmlCompatibility = true;

    public YamlConfiguration() {
        super();
        initialize();
    }

    public YamlConfiguration(final HierarchicalConfiguration c) {
        super(c);
        initialize();
    }

    public YamlConfiguration(final String fileName) throws ConfigurationException {
        super(fileName);
        initialize();
    }

    public YamlConfiguration(final File file) throws ConfigurationException {
        super(file);
        initialize();
    }

    public YamlConfiguration(final URL url) throws ConfigurationException {
        super(url);
        initialize();
    }

    private void initialize() {
        yamlOptions.setIndent(DEFAULT_IDENT);
        yamlOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    }

    public void setXmlCompatibility(final boolean xmlCompatibility) {
        this.xmlCompatibility = xmlCompatibility;
    }

    @Override
    public void load(final Reader in) throws ConfigurationException {
        try {
            this.loadHierarchy(this.getRootNode(), yaml.load(in));
        } catch (Throwable e) {
            throw new ConfigurationException("Failed to load configuration: " + e.getMessage(), e);
        }
    }

    @Override
    public void save(final Writer out) throws ConfigurationException {
        try {
            yaml.dump(this.saveHierarchy(this.getRootNode()), out);
        } catch (Throwable e) {
            throw new ConfigurationException("Failed to save configuration: " + e.getMessage(), e);
        }
    }

    protected void loadHierarchy(final ConfigurationNode parentNode, final Object obj) {
        final String parentName = parentNode.getName();
        if (obj instanceof Map<?, ?>) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) obj).entrySet()) {
                final Node childNode = new Node(entry.getKey());

                // if parent node is look like "tableS", "userS" or "groupS"
                if (this.xmlCompatibility && parentName != null && parentName.endsWith("s")) {
                    //this is done to have "users.user[@name='smith'] instead of "users.smith"
                    childNode.setName(parentName.substring(0, parentName.length() - 1));
                    childNode.addAttribute(new Node("name", entry.getKey()));
                }

                childNode.setReference(entry);
                loadHierarchy(childNode, entry.getValue());
                parentNode.addChild(childNode);
            }
        } else if (obj instanceof Collection) {
            for (Object child : (Collection) obj) {
                final Node childNode = new Node("item");
                childNode.setReference(child);
                loadHierarchy(childNode, child);
                parentNode.addChild(childNode);
            }
        }

        parentNode.setValue(obj);
    }

    protected Object saveHierarchy(final ConfigurationNode parentNode) {
        if (parentNode.getChildrenCount() == 0)
            return parentNode.getValue();

        if (parentNode.getChildrenCount("item") == parentNode.getChildrenCount()) {
            return parentNode.getChildren().stream().map(this::saveHierarchy).collect(Collectors.toList());
        } else {
            final Map<String, Object> map = new LinkedHashMap<>();
            for (ConfigurationNode childNode : parentNode.getChildren()) {
                String nodeName = childNode.getName();
                if (this.xmlCompatibility && childNode.getAttributes("name").size() > 0)
                    nodeName = String.valueOf(childNode.getAttributes("name").get(0).getValue());


                map.put(nodeName, saveHierarchy(childNode));
            }

            return map;
        }
    }
}
