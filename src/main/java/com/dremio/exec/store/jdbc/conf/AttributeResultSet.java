package com.dremio.exec.store.jdbc.conf;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * A ResultSet that returns LDAP attributes as column metadata.
 * Used to expose configured attributes as columns in Dremio.
 * Returns columns for each table (object class) + attribute combination.
 */
public class AttributeResultSet implements ResultSet {
    // Standard column metadata columns
    private static final String[] META_COLUMNS = {
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
        "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
        "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
        "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
    };

    // Each row is [tableName, attributeName, ordinalPosition]
    private final List<String[]> rows;
    private int currentIndex = -1;
    private boolean closed = false;

    public AttributeResultSet(String[] objectClasses, String[] attributes, String tableNamePattern, String columnNamePattern) {
        this.rows = new ArrayList<>();
        String tablePattern = tableNamePattern != null ? tableNamePattern.replace("%", ".*") : ".*";
        String colPattern = columnNamePattern != null ? columnNamePattern.replace("%", ".*") : ".*";

        // Generate rows for each table + attribute combination
        for (String table : objectClasses) {
            if (table == null || table.isEmpty() || !table.matches(tablePattern)) {
                continue;
            }
            int ordinal = 1;
            for (String attr : attributes) {
                if (attr != null && !attr.isEmpty() && attr.matches(colPattern)) {
                    rows.add(new String[]{table, attr, String.valueOf(ordinal++)});
                }
            }
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (currentIndex < rows.size() - 1) {
            currentIndex++;
            return true;
        }
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (currentIndex < 0 || currentIndex >= rows.size()) {
            throw new SQLException("No current row");
        }
        String[] row = rows.get(currentIndex);
        switch (columnIndex) {
            case 1: return null;  // TABLE_CAT
            case 2: return null;  // TABLE_SCHEM
            case 3: return row[0];  // TABLE_NAME
            case 4: return row[1];  // COLUMN_NAME
            case 5: return String.valueOf(Types.VARCHAR);  // DATA_TYPE
            case 6: return "VARCHAR";  // TYPE_NAME
            case 7: return "65535";  // COLUMN_SIZE
            case 8: return null;  // BUFFER_LENGTH
            case 9: return "0";  // DECIMAL_DIGITS
            case 10: return "10";  // NUM_PREC_RADIX
            case 11: return String.valueOf(DatabaseMetaData.columnNullable);  // NULLABLE
            case 12: return "LDAP Attribute";  // REMARKS
            case 13: return null;  // COLUMN_DEF
            case 14: return null;  // SQL_DATA_TYPE
            case 15: return null;  // SQL_DATETIME_SUB
            case 16: return "65535";  // CHAR_OCTET_LENGTH
            case 17: return row[2];  // ORDINAL_POSITION
            case 18: return "YES";  // IS_NULLABLE
            case 19: return null;  // SCOPE_CATALOG
            case 20: return null;  // SCOPE_SCHEMA
            case 21: return null;  // SCOPE_TABLE
            case 22: return null;  // SOURCE_DATA_TYPE
            case 23: return "NO";  // IS_AUTOINCREMENT
            case 24: return "NO";  // IS_GENERATEDCOLUMN
            default: return null;
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String val = getString(columnIndex);
        if (val == null) return 0;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) getInt(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < META_COLUMNS.length; i++) {
            if (META_COLUMNS[i].equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new AttributeResultSetMetaData();
    }

    @Override public Object getObject(int columnIndex) throws SQLException { return getString(columnIndex); }
    @Override public Object getObject(String columnLabel) throws SQLException { return getString(columnLabel); }

    // Minimal implementations
    @Override public boolean getBoolean(int columnIndex) throws SQLException { return false; }
    @Override public byte getByte(int columnIndex) throws SQLException { return 0; }
    @Override public long getLong(int columnIndex) throws SQLException { return getInt(columnIndex); }
    @Override public float getFloat(int columnIndex) throws SQLException { return 0; }
    @Override public double getDouble(int columnIndex) throws SQLException { return 0; }
    @Override @Deprecated public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { return null; }
    @Override public byte[] getBytes(int columnIndex) throws SQLException { return null; }
    @Override public Date getDate(int columnIndex) throws SQLException { return null; }
    @Override public Time getTime(int columnIndex) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(int columnIndex) throws SQLException { return null; }
    @Override public InputStream getAsciiStream(int columnIndex) throws SQLException { return null; }
    @Override @Deprecated public InputStream getUnicodeStream(int columnIndex) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(int columnIndex) throws SQLException { return null; }

    @Override public boolean getBoolean(String columnLabel) throws SQLException { return false; }
    @Override public byte getByte(String columnLabel) throws SQLException { return 0; }
    @Override public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
    @Override public float getFloat(String columnLabel) throws SQLException { return 0; }
    @Override public double getDouble(String columnLabel) throws SQLException { return 0; }
    @Override @Deprecated public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { return null; }
    @Override public byte[] getBytes(String columnLabel) throws SQLException { return null; }
    @Override public Date getDate(String columnLabel) throws SQLException { return null; }
    @Override public Time getTime(String columnLabel) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(String columnLabel) throws SQLException { return null; }
    @Override public InputStream getAsciiStream(String columnLabel) throws SQLException { return null; }
    @Override @Deprecated public InputStream getUnicodeStream(String columnLabel) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(String columnLabel) throws SQLException { return null; }

    @Override public SQLWarning getWarnings() throws SQLException { return null; }
    @Override public void clearWarnings() throws SQLException { }
    @Override public String getCursorName() throws SQLException { return null; }
    @Override public Reader getCharacterStream(int columnIndex) throws SQLException { return null; }
    @Override public Reader getCharacterStream(String columnLabel) throws SQLException { return null; }
    @Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException { return null; }
    @Override public BigDecimal getBigDecimal(String columnLabel) throws SQLException { return null; }

    @Override public boolean isBeforeFirst() throws SQLException { return currentIndex < 0; }
    @Override public boolean isAfterLast() throws SQLException { return currentIndex >= rows.size(); }
    @Override public boolean isFirst() throws SQLException { return currentIndex == 0; }
    @Override public boolean isLast() throws SQLException { return currentIndex == rows.size() - 1; }
    @Override public void beforeFirst() throws SQLException { currentIndex = -1; }
    @Override public void afterLast() throws SQLException { currentIndex = rows.size(); }
    @Override public boolean first() throws SQLException { if (rows.isEmpty()) return false; currentIndex = 0; return true; }
    @Override public boolean last() throws SQLException { if (rows.isEmpty()) return false; currentIndex = rows.size() - 1; return true; }
    @Override public int getRow() throws SQLException { return currentIndex + 1; }
    @Override public boolean absolute(int row) throws SQLException { return false; }
    @Override public boolean relative(int rows) throws SQLException { return false; }
    @Override public boolean previous() throws SQLException { if (currentIndex > 0) { currentIndex--; return true; } return false; }

    @Override public void setFetchDirection(int direction) throws SQLException { }
    @Override public int getFetchDirection() throws SQLException { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) throws SQLException { }
    @Override public int getFetchSize() throws SQLException { return 0; }
    @Override public int getType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() throws SQLException { return ResultSet.CONCUR_READ_ONLY; }

    @Override public boolean rowUpdated() throws SQLException { return false; }
    @Override public boolean rowInserted() throws SQLException { return false; }
    @Override public boolean rowDeleted() throws SQLException { return false; }

    @Override public void updateNull(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(int columnIndex, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(int columnIndex, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(int columnIndex, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(int columnIndex, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(int columnIndex, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(int columnIndex, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(int columnIndex, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(int columnIndex, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(int columnIndex, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int columnIndex, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override public void updateNull(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(String columnLabel, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(String columnLabel, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(String columnLabel, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(String columnLabel, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(String columnLabel, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(String columnLabel, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(String columnLabel, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(String columnLabel, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(String columnLabel, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(String columnLabel, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(String columnLabel, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String columnLabel, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override public void insertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void deleteRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void refreshRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void cancelRowUpdates() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToInsertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToCurrentRow() throws SQLException { }

    @Override public Statement getStatement() throws SQLException { return null; }
    @Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException { return getString(columnIndex); }
    @Override public Ref getRef(int columnIndex) throws SQLException { return null; }
    @Override public Blob getBlob(int columnIndex) throws SQLException { return null; }
    @Override public Clob getClob(int columnIndex) throws SQLException { return null; }
    @Override public Array getArray(int columnIndex) throws SQLException { return null; }
    @Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { return getString(columnLabel); }
    @Override public Ref getRef(String columnLabel) throws SQLException { return null; }
    @Override public Blob getBlob(String columnLabel) throws SQLException { return null; }
    @Override public Clob getClob(String columnLabel) throws SQLException { return null; }
    @Override public Array getArray(String columnLabel) throws SQLException { return null; }
    @Override public Date getDate(int columnIndex, Calendar cal) throws SQLException { return null; }
    @Override public Date getDate(String columnLabel, Calendar cal) throws SQLException { return null; }
    @Override public Time getTime(int columnIndex, Calendar cal) throws SQLException { return null; }
    @Override public Time getTime(String columnLabel, Calendar cal) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { return null; }
    @Override public URL getURL(int columnIndex) throws SQLException { return null; }
    @Override public URL getURL(String columnLabel) throws SQLException { return null; }

    @Override public void updateRef(int columnIndex, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRef(String columnLabel, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int columnIndex, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String columnLabel, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int columnIndex, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String columnLabel, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(int columnIndex, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(String columnLabel, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override public RowId getRowId(int columnIndex) throws SQLException { return null; }
    @Override public RowId getRowId(String columnLabel) throws SQLException { return null; }
    @Override public void updateRowId(int columnIndex, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(String columnLabel, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public int getHoldability() throws SQLException { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public boolean isClosed() throws SQLException { return closed; }

    @Override public void updateNString(int columnIndex, String nString) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(String columnLabel, String nString) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String columnLabel, NClob nClob) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(int columnIndex) throws SQLException { return null; }
    @Override public NClob getNClob(String columnLabel) throws SQLException { return null; }
    @Override public SQLXML getSQLXML(int columnIndex) throws SQLException { return null; }
    @Override public SQLXML getSQLXML(String columnLabel) throws SQLException { return null; }
    @Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String getNString(int columnIndex) throws SQLException { return getString(columnIndex); }
    @Override public String getNString(String columnLabel) throws SQLException { return getString(columnLabel); }
    @Override public Reader getNCharacterStream(int columnIndex) throws SQLException { return null; }
    @Override public Reader getNCharacterStream(String columnLabel) throws SQLException { return null; }

    @Override public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int columnIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int columnIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException { return null; }
    @Override public <T> T getObject(String columnLabel, Class<T> type) throws SQLException { return null; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    /**
     * Metadata for AttributeResultSet.
     */
    private static class AttributeResultSetMetaData implements ResultSetMetaData {
        @Override public int getColumnCount() throws SQLException { return META_COLUMNS.length; }
        @Override public boolean isAutoIncrement(int column) throws SQLException { return false; }
        @Override public boolean isCaseSensitive(int column) throws SQLException { return true; }
        @Override public boolean isSearchable(int column) throws SQLException { return true; }
        @Override public boolean isCurrency(int column) throws SQLException { return false; }
        @Override public int isNullable(int column) throws SQLException { return ResultSetMetaData.columnNullable; }
        @Override public boolean isSigned(int column) throws SQLException { return false; }
        @Override public int getColumnDisplaySize(int column) throws SQLException { return 255; }
        @Override public String getColumnLabel(int column) throws SQLException { return META_COLUMNS[column - 1]; }
        @Override public String getColumnName(int column) throws SQLException { return META_COLUMNS[column - 1]; }
        @Override public String getSchemaName(int column) throws SQLException { return ""; }
        @Override public int getPrecision(int column) throws SQLException { return 0; }
        @Override public int getScale(int column) throws SQLException { return 0; }
        @Override public String getTableName(int column) throws SQLException { return ""; }
        @Override public String getCatalogName(int column) throws SQLException { return ""; }
        @Override public int getColumnType(int column) throws SQLException { return Types.VARCHAR; }
        @Override public String getColumnTypeName(int column) throws SQLException { return "VARCHAR"; }
        @Override public boolean isReadOnly(int column) throws SQLException { return true; }
        @Override public boolean isWritable(int column) throws SQLException { return false; }
        @Override public boolean isDefinitelyWritable(int column) throws SQLException { return false; }
        @Override public String getColumnClassName(int column) throws SQLException { return String.class.getName(); }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("Cannot unwrap");
        }
        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return iface.isInstance(this); }
    }
}
