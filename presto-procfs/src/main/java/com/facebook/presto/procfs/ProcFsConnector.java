package com.facebook.presto.procfs;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import static com.facebook.presto.procfs.ProcFsTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

import javax.inject.Inject;

public class ProcFsConnector implements Connector {
    private static final Logger log = Logger.get(ProcFsConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ProcFsMetadata metadata;
    private final ProcFsSplitManager splitManager;
    private final ProcFsRecordSetProvider recordSetProvider;

    @Inject
    public ProcFsConnector(
            LifeCycleManager lifeCycleManager,
            ProcFsMetadata metadata,
            ProcFsSplitManager splitManager,
            ProcFsRecordSetProvider recordSetProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
