package com.facebook.presto.kubernetes;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public enum KubernetesTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
