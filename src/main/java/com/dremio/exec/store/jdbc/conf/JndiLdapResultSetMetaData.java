package com.dremio.exec.store.jdbc.conf;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * ResultSetMetaData implementation for JNDI LDAP results.
 * All LDAP attributes are treated as VARCHAR strings.
 */
public class JndiLdapResultSetMetaData implements ResultSetMetaData {

    private final String[] columnNames;

    public JndiLdapResultSetMetaData(String[] columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.length;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumn(column);
        return columnNames[column - 1];
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return "VARCHAR";
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return String.class.getName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 4000; // Max LDAP attribute length
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 255;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "ldap";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    private void checkColumn(int column) throws SQLException {
        if (column < 1 || column > columnNames.length) {
            throw new SQLException("Invalid column index: " + column);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
