package com.facebook.presto.procfs;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class ProcFsConnectorId {
    private final String id;

    public ProcFsConnectorId(String id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ProcFsConnectorId other = (ProcFsConnectorId) obj;
        return Objects.equals(this.id, other.id);
    }
}
