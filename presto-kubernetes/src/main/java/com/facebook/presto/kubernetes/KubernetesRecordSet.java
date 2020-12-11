package com.facebook.presto.kubernetes;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import io.kubernetes.client.openapi.ApiException;

import java.net.MalformedURLException;
import java.util.List;

public class KubernetesRecordSet
        implements RecordSet
{
    private final List<KubernetesColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final List<String> namespaces;

    public KubernetesRecordSet(KubernetesSplit split,
                               List<KubernetesColumnHandle> columnHandles,
                               KubernetesClient kubernetesClient)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KubernetesColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();


        try {
            namespaces = kubernetesClient.getRows(split.getSchemaName(), split.getTableName());
        }
        catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KubernetesRecordCursor(columnHandles, namespaces.iterator());
    }
}
