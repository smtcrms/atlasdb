/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.config;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Strings;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;

public final class AtlasDbConfigs {
    public static final String ATLASDB_CONFIG_ROOT = "atlasdb";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    static {
        OBJECT_MAPPER.setSubtypeResolver(new DiscoverableSubtypeResolver());
        OBJECT_MAPPER.registerModule(new GuavaModule());
    }

    private AtlasDbConfigs() {
        // uninstantiable
    }

    public static AtlasDbConfig load(File configFile) throws IOException {
        return load(configFile, ATLASDB_CONFIG_ROOT);
    }

    public static AtlasDbConfig load(File configFile, String configRoot) throws IOException {
        JsonNode rootNode = getConfigNode(configFile, configRoot);
        return OBJECT_MAPPER.treeToValue(rootNode, AtlasDbConfig.class);
    }

    public static AtlasDbConfig loadFromString(String fileContents, String configRoot) throws IOException {
        JsonNode rootNode = getConfigNode(fileContents, configRoot);
        return OBJECT_MAPPER.treeToValue(rootNode, AtlasDbConfig.class);
    }

    private static JsonNode getConfigNode(File configFile, String configRoot) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(configFile);
        JsonNode configNode = getConfigNodeUnsafe(node, configRoot);

        if (configNode == null) {
            throw new IllegalArgumentException("Could not find " + configRoot + " in yaml file " + configFile);
        }

        return configNode;
    }

    private static JsonNode getConfigNode(String fileContents, String configRoot) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(fileContents);
        JsonNode configNode = getConfigNodeUnsafe(node, configRoot);

        if (configNode == null) {
            throw new IllegalArgumentException("Could not find " + configRoot + " in given string");
        }

        return configNode;
    }

    private static JsonNode getConfigNodeUnsafe(JsonNode node, String configRoot) {
        if (Strings.isNullOrEmpty(configRoot)) {
            return node;
        } else {
            return findRoot(node, configRoot);
        }
    }

    private static JsonNode findRoot(JsonNode node, String configRoot) {
        if (node.has(configRoot)) {
            return node.get(configRoot);
        } else {
            Iterator<String> iter = node.fieldNames();
            while (iter.hasNext()) {
                JsonNode root = findRoot(node.get(iter.next()), configRoot);
                if (root != null) {
                    return root;
                }
            }
            return null;
        }
    }
}
