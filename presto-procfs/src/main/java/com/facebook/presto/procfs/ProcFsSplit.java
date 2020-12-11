package com.facebook.presto.procfs;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ProcFsSplit implements ConnectorSplit {

    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final HostAddress host;

    @JsonCreator
    public ProcFsSplit(
            @JsonProperty ("connectorId") String connectorId,
            @JsonProperty ("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("host") HostAddress host)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.host = requireNonNull(host, "host is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public HostAddress getHost()
    {
        return host;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of(host);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
