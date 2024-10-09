/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.Map;

/**
 * Visitor pattern for obtaining index mappings
 *
 * @opensearch.internal
 */
final class MappingVisitor {

    private MappingVisitor() {}

    @SuppressWarnings("unchecked")
    static String visitMapping(Map<String, ?> propertiesAsMap, String[] fieldName, int idx) {
        if (propertiesAsMap.containsKey(fieldName[idx])) {
            Map<String, ?> fieldMapping = (Map<String, ?>) propertiesAsMap.get(fieldName[idx]);
            if (idx == fieldName.length - 1) {
                return (String) fieldMapping.get("type");
            } else {
                // Multi field case
                if (fieldMapping.containsKey("fields")) {
                    return visitMapping((Map<String, ?>) fieldMapping.get("fields"), fieldName, idx + 1);
                } else {
                    return null;
                }
            }
        }

        // fieldName not found at current level
        // call visitMapping() for any fields with subfields (contains "properties" key)
        for (Object v : propertiesAsMap.values()) {
            if (v instanceof Map) {
                Map<String, ?> fieldMapping = (Map<String, ?>) v;
                if (fieldMapping.containsKey("properties")) {
                    return visitMapping((Map<String, ?>) fieldMapping.get("properties"), fieldName, idx);
                }
            }
        }
        // no mapping found
        return null;
    }

}
