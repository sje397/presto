package com.facebook.presto.procfs;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

public class ProcFsRecordSet
        implements RecordSet
{
    private final List<ProcFsColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final List<String> data;

    public ProcFsRecordSet(ProcFsSplit split,
                               List<ProcFsColumnHandle> columnHandles,
                               ProcFsClient procFsClient)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ProcFsColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();


        try {
            data = procFsClient.getRows(split.getSchemaName(), split.getTableName(), split.getHost());
        }
        catch (Exception e) {
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
        return new ProcFsRecordCursor(columnHandles, data.iterator());
    }
}
