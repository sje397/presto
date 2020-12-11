package com.facebook.presto.procfs;

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

public class ProcFsRecordSetProvider implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final ProcFsClient procFsClient;

    @Inject
    public ProcFsRecordSetProvider(ProcFsConnectorId connectorId,
                                       ProcFsClient procFsClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.procFsClient = requireNonNull(procFsClient, "procFsClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        requireNonNull(split, "partitionChunk is null");
        ProcFsSplit procFsSplit = (ProcFsSplit) split;
        checkArgument(procFsSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<ProcFsColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((ProcFsColumnHandle) handle);
        }

        return new ProcFsRecordSet(procFsSplit, handles.build(), procFsClient);
    }
}
