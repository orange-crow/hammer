package com.hammer.config;

import com.google.gson.Gson;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ConfigLoader {
    private static final String CONFIG_FILE = "/config.json";
    public static AppConfig loadConfig() {
        try (InputStream inputStream = ConfigLoader.class.getResourceAsStream(CONFIG_FILE)) {
            if (inputStream == null) {
                throw new RuntimeException("Cofing file not found" + CONFIG_FILE);
            }
            InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            Gson json = new Gson();
            return json.fromJson(reader, AppConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Faild to load config file", e);
        }
    }
}
