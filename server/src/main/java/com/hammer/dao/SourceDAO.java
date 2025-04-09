package com.hammer.dao;

import com.hammer.entity.Source;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.*;
import java.util.Map;

import com.hammer.config.ConfigLoader;
import com.hammer.config.AppConfig;


public class SourceDAO {
    private static final AppConfig config = ConfigLoader.loadConfig();
    private static final String URL = config.getHammerMeta().getUrl();
    private static final String USER = config.getHammerMeta().getUser();
    private static final String PASSWORD = config.getHammerMeta().getPassword();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Source getSource(String name, String version) {
        String sql = "SELECT name, version, table_name, infra_type, field_mapping, owner, description, config " +
                     "FROM source WHERE name = ? AND version = ?";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, name);
            stmt.setString(2, version);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String tableName = rs.getString("table_name");
                String infraType = rs.getString("infra_type");
                String fieldMappingJson = rs.getString("field_mapping");
                String owner = rs.getString("owner");
                String description = rs.getString("description");
                String configJson = rs.getString("config");

                // 使用 ObjectMapper 将 JSON 字符串解析为 Map<String, String>
                @SuppressWarnings("unchecked")
                Map<String, String> fieldMapping = objectMapper.readValue(fieldMappingJson, Map.class);

                @SuppressWarnings("unchecked")
                Map<String, String> config = objectMapper.readValue(configJson, Map.class);

                return new Source(name, version, tableName, infraType, fieldMapping, owner, description, config);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null; // 如果没有找到数据
    }

    public Source getSource(Map<String, String> sourceMap) {
        String name = sourceMap.get("name");
        String version = sourceMap.get("version");
        return getSource(name, version);
    }
}
