package com.facebook.presto.kubernetes;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class KubernetesTableLayoutHandle implements ConnectorTableLayoutHandle
{
    private final KubernetesTableHandle table;

    @JsonCreator
    public KubernetesTableLayoutHandle(@JsonProperty("table") KubernetesTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public KubernetesTableHandle getTable()
    {
        return table;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KubernetesTableLayoutHandle that = (KubernetesTableLayoutHandle) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}