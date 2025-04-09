package com.hammer.entity;

import lombok.Data;
import java.util.List;


@Data
public class Entity {
    private String name;
    private List<String> joinKeys;

    public Entity(String name, List<String> joinKeys) {
        this.name = name;
        this.joinKeys = joinKeys;
    }
}
