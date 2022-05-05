package com.asardaes.flink.utils;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.java.functions.KeySelector;

public class PojoKeySelector implements KeySelector<Pojo, String> {
    @Override
    public String getKey(Pojo value) {
        return value.key;
    }
}
