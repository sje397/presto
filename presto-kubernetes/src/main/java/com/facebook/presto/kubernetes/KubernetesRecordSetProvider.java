package com.facebook.presto.kubernetes;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;

import javax.inject.Inject;

public class KubernetesRecordSetProvider implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final KubernetesClient kubernetesClient;

    @Inject
    public KubernetesRecordSetProvider(KubernetesConnectorId connectorId,
                                       KubernetesClient kubernetesClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.kubernetesClient = requireNonNull(kubernetesClient, "kubernetesClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        requireNonNull(split, "partitionChunk is null");
        KubernetesSplit kubernetesSplit = (KubernetesSplit) split;
        checkArgument(kubernetesSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<KubernetesColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((KubernetesColumnHandle) handle);
        }

        return new KubernetesRecordSet(kubernetesSplit, handles.build(), kubernetesClient);
    }
}