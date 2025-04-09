package com.hammer.entity;

import lombok.Data;
import java.util.Map;


@Data
public class Source {
    private String name;
    private String version;
    private String tableName;
    private String infraType;
    private Map<String, String> fieldMapping;
    private String owner;
    private String description;
    private Map<String, String> config;

    public Source(String name, String version, String tableName, String infraType, Map<String, String> fieldMapping,
                  String owner, String description, Map<String, String> config) {
        this.name = name;
        this.version = version;
        this.tableName = tableName;
        this.infraType = infraType;
        this.fieldMapping = fieldMapping;
        this.owner = owner;
        this.description = description;
        this.config = config;
    }

    @Override
    public String toString() {
        return String.format("<Source %s:%s:%s>", name, version, tableName);
    }
}
