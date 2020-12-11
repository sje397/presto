package com.facebook.presto.procfs;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ProcFsTableLayoutHandle implements ConnectorTableLayoutHandle
{
    private final ProcFsTableHandle table;

    @JsonCreator
    public ProcFsTableLayoutHandle(@JsonProperty("table") ProcFsTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public ProcFsTableHandle getTable()
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
        ProcFsTableLayoutHandle that = (ProcFsTableLayoutHandle) o;
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
