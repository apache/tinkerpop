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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.shaded.jackson.core.JsonParseException;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.util.JsonParserSequence;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Utility class to easily concatenate multiple JsonParsers. This class had to be implemented because the class it is
 * extending, {@link JsonParserSequence}, inevitably skips a token when switching from one empty parser to a new one.
 * I.e. it is automatically calling {@link JsonParser#nextToken()} when switching to the new parser, ignoring
 * the current token.
 *
 * This class is used for high performance in GraphSON when trying to detect types.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 */
public class JsonParserConcat extends JsonParserSequence {
    protected JsonParserConcat(final JsonParser[] parsers) {
        super(parsers);
    }

    public static JsonParserConcat createFlattened(final JsonParser first, final JsonParser second) {
        if (!(first instanceof JsonParserConcat) && !(second instanceof JsonParserConcat)) {
            return new JsonParserConcat(new JsonParser[]{first, second});
        } else {
            final ArrayList p = new ArrayList();
            if (first instanceof JsonParserConcat) {
                ((JsonParserConcat) first).addFlattenedActiveParsers(p);
            } else {
                p.add(first);
            }

            if (second instanceof JsonParserConcat) {
                ((JsonParserConcat) second).addFlattenedActiveParsers(p);
            } else {
                p.add(second);
            }
            return new JsonParserConcat((JsonParser[]) p.toArray(new JsonParser[p.size()]));
        }
    }

    @Override
    public JsonToken nextToken() throws IOException, JsonParseException {
        JsonToken t = this.delegate.nextToken();
        if (t != null) {
            return t;
        } else {
            do {
                if (!this.switchToNext()) {
                    return null;
                }
                // call getCurrentToken() instead of nextToken() in JsonParserSequence.
                t = this.delegate.getCurrentToken() == null
                        ? this.nextToken()
                        : this.getCurrentToken();
            } while (t == null);

            return t;
        }
    }
}
