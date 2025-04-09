package com.hammer.dao;

import java.sql.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hammer.config.ConfigLoader;
import com.hammer.entity.Entity;
import com.hammer.config.AppConfig;


public class EntityDAO {

    private static final AppConfig config = ConfigLoader.loadConfig();

    private static final String URL = config.getHammerMeta().getUrl();
    private static final String USER = config.getHammerMeta().getUser();
    private static final String PASSWORD = config.getHammerMeta().getPassword();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Entity getEntityByName(String name) {
        String sql = "SELECT name, join_keys FROM entity WHERE name = ?";

        try (
            Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
            PreparedStatement stmt = conn.prepareStatement(sql)
        ) {
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String entityName = rs.getString("name");
                String joinKeysJson = rs.getString("join_keys");

                List<String> joinKeys = objectMapper.readValue(joinKeysJson, new TypeReference<List<String>>() {});
                return new Entity(entityName, joinKeys);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
