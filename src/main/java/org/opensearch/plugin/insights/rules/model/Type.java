package org.opensearch.plugin.insights.rules.model;

/**
 * Type of the search query record. Can be either "query" or "group"
 */
public enum Type {
    query("query"),
    group("group");

    private final String stringValue;

    Type(String stringValue) {
        this.stringValue = stringValue;
    }

    public String getValue() {
        return stringValue;
    }
}
