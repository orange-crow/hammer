package com.hammer.config;

import lombok.Data;


@Data
public class AppConfig {
    private DatabaseConfig hammerMeta;  // 与JSON字段名映射, 若不同，则使用 @SerializedName("hammer_meta")
    private DatabaseConfig feature;

    @Data
    public static class DatabaseConfig {
        private String url;
        private String user;
        private String password;
    }
}
