package com.facebook.presto.procfs;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

public class ProcFsSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(ProcFsSplitManager.class);

    private final String connectorId;
    private final ProcFsClient procFsClient;
    private final List<HostAddress> hosts;

    @Inject
    public ProcFsSplitManager(ProcFsConnectorId connectorId, ProcFsClient procFsClient,
                              ProcFsConfig config) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.procFsClient = requireNonNull(procFsClient, "procFsClient is null");
        this.hosts = config.getHosts();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle handle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        ProcFsTableLayoutHandle layoutHandle = (ProcFsTableLayoutHandle) layout;
        ProcFsTableHandle tableHandle = layoutHandle.getTable();
        ProcFsTable table = procFsClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        for(HostAddress host: hosts) {
            ProcFsSplit split = new ProcFsSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), host);
            log.info("Created split for host %s table %s.%s", host.getHostText(), tableHandle.getSchemaName(), tableHandle.getTableName());
            splits.add(split);
        }

        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
