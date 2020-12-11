package com.facebook.presto.kubernetes;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

public class KubernetesSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(KubernetesSplitManager.class);

    private final String connectorId;
    private final KubernetesClient kubernetesClient;

    @Inject
    public KubernetesSplitManager(KubernetesConnectorId connectorId, KubernetesClient kubernetesClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.kubernetesClient = requireNonNull(kubernetesClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle handle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        KubernetesTableLayoutHandle layoutHandle = (KubernetesTableLayoutHandle) layout;
        KubernetesTableHandle tableHandle = layoutHandle.getTable();
        KubernetesTable table = kubernetesClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        KubernetesSplit split = new KubernetesSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName());
        log.info("Created split for %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        splits.add(split);

        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
