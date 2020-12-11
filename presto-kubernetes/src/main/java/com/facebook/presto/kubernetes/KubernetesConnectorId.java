package com.facebook.presto.kubernetes;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class KubernetesConnectorId {
    private final String id;

    public KubernetesConnectorId(String id)
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

        KubernetesConnectorId other = (KubernetesConnectorId) obj;
        return Objects.equals(this.id, other.id);
    }
}
