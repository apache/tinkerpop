package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphVariables implements Graph.Variables, Serializable {

    private final Map<String, Object> variables = new HashMap<>();

    public GiraphGraphVariables(final Configuration configuration) {
        this.variables.put(Constants.CONFIGURATION, configuration);
    }

    public Set<String> keys() {
        return this.variables.keySet();
    }

    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.variables.get(key));
    }

    public void remove(final String key) {
        this.variables.remove(key);
    }

    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.variables.put(key, value);
    }

    public GiraphConfiguration getConfiguration() {
        return this.<GiraphConfiguration>get(Constants.CONFIGURATION).get();
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
