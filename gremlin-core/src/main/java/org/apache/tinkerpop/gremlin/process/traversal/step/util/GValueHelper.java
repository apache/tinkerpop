package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;

/**
 * Utility class for {@link GValue} helper methods
 */
public class GValueHelper {
    private GValueHelper() {
    }

    /**
     * Resolves given properties by extracting values from any {@link GValue} property values.
     * It is assumed that the property keys are not {@link GValue}s.
     *
     * @param properties map of properties which may have values which are {@link GValue}s
     * @return map of properties with {@link GValue}s resolved to their values
     */
    public static Map<Object, List<Object>> resolveProperties(final Map<Object, List<Object>> properties) {
        return resolveProperties(properties, null);
    }

    /**
     * Resolves given properties by extracting values from any {@link GValue} property values. It is assumed that the property keys are not {@link GValue}s.
     *
     * @param properties     map of properties which may have values which are {@link GValue}s
     * @param gValueConsumer a {@link Consumer} that will be called for each {@link GValue} found in the given properties
     * @return map of properties with {@link GValue}s resolved to their values
     */
    public static Map<Object, List<Object>> resolveProperties(final Map<Object, List<Object>> properties, Consumer<GValue<?>> gValueConsumer) {
        Map<Object, List<Object>> unboxedProperties = new HashMap<>();
        properties.forEach((k, values) -> {
            List<Object> unboxedValues = new ArrayList<>();
            values.forEach(v -> {
                if (v instanceof GValue) {
                    GValue<?> gValue = (GValue<?>) v;
                    if (gValueConsumer != null) {
                        gValueConsumer.accept(gValue);
                    }
                    unboxedValues.add(gValue.get());
                } else {
                    unboxedValues.add(v);
                }
            });
            unboxedProperties.put(k, unboxedValues);
        });
        return unboxedProperties;
    }

    /**
     * Extracts a set of {@link GValue}s found from the given property map that can contain values that are {@link GValue}s.
     * It is assumed that the property keys are not {@link GValue}s.
     *
     * @param properties map of properties which may have values which are {@link GValue}s
     * @return a set of {@link GValue} property values which were found in the given property map
     */
    public static Set<GValue<?>> getGValuesFromProperties(final Map<Object, List<Object>> properties) {
        return properties.values().stream()
                .flatMap(List::stream)
                .filter(propertyVal -> propertyVal instanceof GValue && ((GValue<?>) propertyVal).isVariable())
                .map(obj -> (GValue<?>) obj)
                .collect(Collectors.toSet());
    }
}
