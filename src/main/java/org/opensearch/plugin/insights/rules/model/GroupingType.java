/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Grouping type to group the Top N queries
 */
public enum GroupingType {
    NONE("none"),
    SIMILARITY("similarity"),
    USER_ID("userid");

    private final String stringValue;

    GroupingType(String stringValue) {
        this.stringValue = stringValue;
    }

    public String getValue() {
        return stringValue;
    }

    /**
     * Get all valid GroupingTypes
     *
     * @return A set contains all valid GroupingTypes
     */
    public static Set<GroupingType> allGroupingTypes() {
        return Arrays.stream(values()).collect(Collectors.toSet());
    }

    /**
     * Get grouping type from setting string and validate
     * @param settingValue value
     * @return GroupingType
     */
    public static GroupingType getGroupingTypeFromSettingAndValidate(String settingValue) {
        try {
            return GroupingType.valueOf(settingValue.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Invalid exporter type [%s], type should be one of %s",
                    settingValue,
                    allGroupingTypes()
                )
            );
        }
    }
}
