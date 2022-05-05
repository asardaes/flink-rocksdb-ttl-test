package com.asardaes.flink.dto;

import java.io.Serializable;
import java.util.Objects;

public class Pojo implements Serializable {
    public long epoch;
    public String key;
    public String value;

    private Pojo() {
    }

    public Pojo(String key, String value) {
        this.epoch = System.currentTimeMillis();
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Pojo{" +
                "epoch=" + epoch +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pojo pojo = (Pojo) o;
        return epoch == pojo.epoch && Objects.equals(key, pojo.key) && Objects.equals(value, pojo.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, key, value);
    }
}
