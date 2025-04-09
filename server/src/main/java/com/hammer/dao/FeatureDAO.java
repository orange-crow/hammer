package com.hammer.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.*;
import java.util.Map;

import com.hammer.entity.Feature;
import com.hammer.entity.Source;
import com.hammer.config.ConfigLoader;
import com.hammer.entity.Entity;
import com.hammer.config.AppConfig;


public class FeatureDAO {
    private static final AppConfig config = ConfigLoader.loadConfig();
    private static final String URL = config.getHammerMeta().getUrl();
    private static final String USER = config.getHammerMeta().getUser();
    private static final String PASSWORD = config.getHammerMeta().getPassword();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Feature getFeature(String name, String version, String startEventDatetime, String endEventDatetime) {
        String sql = "SELECT name, version, source, entity, event_timestamp_field, start_event_datetime, end_event_datetime, ttl, " +
                     "schema, sink, transform, description, owner, status " +
                     "FROM feature WHERE name = ? AND version = ?";
        if (startEventDatetime != null && endEventDatetime != null) {
            sql += " AND start_event_datetime >= ? AND end_event_datetime <= ?";
        }
        sql += " ORDER BY start_event_datetime DESC LIMIT 1";

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, name);
            stmt.setString(2, version);
            if (startEventDatetime != null && endEventDatetime != null) {
                stmt.setString(3, startEventDatetime);
                stmt.setString(4, endEventDatetime);
            }
            // 执行查询
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String sourceJson = rs.getString("source");
                String entityJson = rs.getString("entity");
                String eventTimestampField = rs.getString("event_timestamp_field");
                String ttl = rs.getString("ttl");
                String schemaJson = rs.getString("schema");
                String sinkJson = rs.getString("sink");
                String transform = rs.getString("transform");
                String description = rs.getString("description");
                String owner = rs.getString("owner");
                String status = rs.getString("status");

                // 反序列化 Entity JSON 字符串
                @SuppressWarnings("unchecked")
                Map<String, String> entityJMap = objectMapper.readValue(entityJson, Map.class);
                String entityName = entityJMap.get("name");
                EntityDAO edao = new EntityDAO();
                Entity entity = edao.getEntity(entityName);
                if (entity == null) {
                    System.out.println("未找到该 Entity: " + entityName);
                    return null;
                }

                // 反序列化 Source JSON 字符串
                @SuppressWarnings("unchecked")
                Map<String, String> sourceJMap = objectMapper.readValue(sourceJson, Map.class);
                SourceDAO sdao = new SourceDAO();
                Source source = sdao.getSource(sourceJMap);
                if (source == null) {
                    System.out.println("未找到该 Source: " + name + ":" + version);
                    return null;
                }

                @SuppressWarnings("unchecked")
                Map<String, String> schema = objectMapper.readValue(schemaJson, Map.class);
                @SuppressWarnings("unchecked")
                Map<String, String> sink = objectMapper.readValue(sinkJson, Map.class);

                return new Feature(name, version, source, entity, eventTimestampField, startEventDatetime, endEventDatetime,
                        Integer.parseInt(ttl), schema, sink, transform, description, owner, status);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null; // 如果没有找到数据
    }

    public Feature getFeature(Map<String, String> sourceMap) {
        String name = sourceMap.get("name");
        String version = sourceMap.get("version");
        String startEventDatetime = sourceMap.get("start_event_datetime");
        String endEventDateTime = sourceMap.get("end_event_datetime");
        return getFeature(name, version, startEventDatetime, endEventDateTime);
    }
}
