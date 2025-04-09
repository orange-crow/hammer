package com.hammer.entity;

import java.util.Map;
import lombok.Data;


@Data
public class Feature {
    private String name;
    private String version;
    private Source source;
    private Entity entity;
    private String eventTimestampField;
    private String startEventDatetime;
    private String endEventDatetime;
    private int ttl;
    private Map<String, String> schema;  // JSON 结构
    private Map<String, String> sink;    // hive 上的 database, table
    private String transform;            // SQL 代码，传递给 FeatureComputer 进行计算
    private String description;
    private String owner;
    public String status; // 默认值为 "todo"

    public Feature(String name, String version, Source source, Entity entity,
                   String eventTimestampField, String startEventDatetime, String endEventDatetime, int ttl,
                   Map<String, String> schema, Map<String, String> sink, String transform, String description,
                   String owner, String status) {
        this.name = name;
        this.version = version;
        this.source = source;
        this.entity = entity;
        this.eventTimestampField = eventTimestampField;
        this.startEventDatetime = startEventDatetime;
        this.endEventDatetime = endEventDatetime;
        this.ttl = ttl;
        this.schema = schema;
        this.sink = sink;
        this.transform = transform;
        this.description = description;
        this.owner = owner;
        this.status = status;
    }
 
    @Override
    public String toString() {
        return String.format("<Feature %s:%s>", name, version);
    }
}

