package com.asardaes.flink.dto;

import java.io.Serializable;
import java.util.Objects;

public class HashAndEpoch implements Serializable {
    public int hash;
    public long epoch;

    public HashAndEpoch() {
    }

    public HashAndEpoch(int hash, long epoch) {
        this.hash = hash;
        this.epoch = epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashAndEpoch that = (HashAndEpoch) o;
        return hash == that.hash && epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, epoch);
    }
}
