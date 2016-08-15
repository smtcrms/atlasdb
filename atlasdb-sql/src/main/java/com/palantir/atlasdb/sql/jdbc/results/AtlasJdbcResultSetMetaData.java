package com.palantir.atlasdb.sql.jdbc.results;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.sql.jdbc.results.columns.JdbcColumnMetadata;

public class AtlasJdbcResultSetMetaData implements ResultSetMetaData {

    private final List<JdbcColumnMetadata> cols;

    public static AtlasJdbcResultSetMetaData create(List<JdbcColumnMetadata> cols) {
        return new AtlasJdbcResultSetMetaData(ImmutableList.copyOf(cols));
    }

    private AtlasJdbcResultSetMetaData(List<JdbcColumnMetadata> cols) {
        this.cols = cols;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return cols.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return cols.get(column - 1).isRowComp();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return ValueTypes.isSigned(cols.get(column - 1).getValueType());
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        JdbcColumnMetadata col = cols.get(column - 1);
        return Math.max(col.getLabel().length(), ValueTypes.maxDisplaySize(col.getValueType()));
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return cols.get(column - 1).getLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return cols.get(column - 1).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return ValueTypes.getColumnType(cols.get(column - 1).getValueType());
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return cols.get(column - 1).getValueType().getClass().getSimpleName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return cols.get(column - 1).getValueType().getClass().getSimpleName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

}
