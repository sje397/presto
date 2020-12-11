package com.facebook.presto.procfs;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public enum ProcFsTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
