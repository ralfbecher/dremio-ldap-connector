package com.dremio.exec.store.jdbc.conf;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ResultSet implementation backed by LDAP search results from JNDI.
 */
public class JndiLdapResultSet implements ResultSet {
    private static final Logger LOG = Logger.getLogger(JndiLdapResultSet.class.getName());

    private final List<Map<String, Object>> data;
    private final String[] columnNames;
    private final JndiLdapResultSetMetaData metaData;

    private int currentRow = -1;
    private boolean closed = false;
    private boolean wasNull = false;

    public JndiLdapResultSet(List<Map<String, Object>> data, String[] requestedColumns) {
        this.data = data;

        // Build column list - preserve original case of requested columns
        // Use LinkedHashMap to track lowercase -> original case mapping
        Map<String, String> columnMap = new LinkedHashMap<>();

        // Add requested columns first (in order), preserving original case
        for (String col : requestedColumns) {
            columnMap.put(col.toLowerCase(), col);
        }

        // Add any additional columns found in data (if not already present)
        for (Map<String, Object> row : data) {
            for (String key : row.keySet()) {
                String lowerKey = key.toLowerCase();
                if (!columnMap.containsKey(lowerKey)) {
                    columnMap.put(lowerKey, key);
                }
            }
        }

        // Use the original case names for column names
        this.columnNames = columnMap.values().toArray(new String[0]);
        this.metaData = new JndiLdapResultSetMetaData(columnNames);

        LOG.log(Level.WARNING, "JndiLdapResultSet created with " + data.size() + " rows, " + columnNames.length + " columns: " + String.join(", ", columnNames));
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        currentRow++;
        boolean hasNext = currentRow < data.size();
        if (!hasNext) {
            LOG.log(Level.WARNING, "JndiLdapResultSet.next() = false (end of data)");
        }
        return hasNext;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private void checkRow() throws SQLException {
        checkClosed();
        if (currentRow < 0 || currentRow >= data.size()) {
            throw new SQLException("Invalid row position: " + currentRow);
        }
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        String lowerLabel = columnLabel.toLowerCase();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equalsIgnoreCase(lowerLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    private Object getColumnValue(int columnIndex) throws SQLException {
        checkRow();
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }

        String colName = columnNames[columnIndex - 1];
        Map<String, Object> row = data.get(currentRow);

        // Try exact match first
        Object value = row.get(colName);
        if (value == null) {
            // Try case-insensitive match
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(colName)) {
                    value = entry.getValue();
                    break;
                }
            }
        }

        wasNull = (value == null);
        return value;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        if (value == null) return null;
        return value.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public int getRow() throws SQLException {
        return currentRow + 1;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentRow < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return currentRow >= data.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentRow == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return currentRow == data.size() - 1;
    }

    // Type conversion methods - convert everything to string since LDAP attributes are strings

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String val = getString(columnIndex);
        return val != null && (val.equalsIgnoreCase("true") || val.equals("1"));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
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
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String val = getString(columnIndex);
        if (val == null) return 0;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getColumnValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    // Minimal implementations for other required methods

    @Override public byte getByte(int columnIndex) throws SQLException { return (byte) getInt(columnIndex); }
    @Override public byte getByte(String columnLabel) throws SQLException { return getByte(findColumn(columnLabel)); }
    @Override public short getShort(int columnIndex) throws SQLException { return (short) getInt(columnIndex); }
    @Override public short getShort(String columnLabel) throws SQLException { return getShort(findColumn(columnLabel)); }
    @Override public float getFloat(int columnIndex) throws SQLException { return (float) getDouble(columnIndex); }
    @Override public float getFloat(String columnLabel) throws SQLException { return getFloat(findColumn(columnLabel)); }
    @Override public double getDouble(int columnIndex) throws SQLException {
        String val = getString(columnIndex);
        if (val == null) return 0;
        try { return Double.parseDouble(val); } catch (NumberFormatException e) { return 0; }
    }
    @Override public double getDouble(String columnLabel) throws SQLException { return getDouble(findColumn(columnLabel)); }
    @Override public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { return getBigDecimal(columnIndex); }
    @Override public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { return getBigDecimal(columnLabel); }
    @Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String val = getString(columnIndex);
        if (val == null) return null;
        try { return new BigDecimal(val); } catch (NumberFormatException e) { return null; }
    }
    @Override public BigDecimal getBigDecimal(String columnLabel) throws SQLException { return getBigDecimal(findColumn(columnLabel)); }
    @Override public byte[] getBytes(int columnIndex) throws SQLException {
        String val = getString(columnIndex);
        return val != null ? val.getBytes() : null;
    }
    @Override public byte[] getBytes(String columnLabel) throws SQLException { return getBytes(findColumn(columnLabel)); }
    @Override public Date getDate(int columnIndex) throws SQLException { return null; }
    @Override public Date getDate(String columnLabel) throws SQLException { return null; }
    @Override public Date getDate(int columnIndex, Calendar cal) throws SQLException { return null; }
    @Override public Date getDate(String columnLabel, Calendar cal) throws SQLException { return null; }
    @Override public Time getTime(int columnIndex) throws SQLException { return null; }
    @Override public Time getTime(String columnLabel) throws SQLException { return null; }
    @Override public Time getTime(int columnIndex, Calendar cal) throws SQLException { return null; }
    @Override public Time getTime(String columnLabel, Calendar cal) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(int columnIndex) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(String columnLabel) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { return null; }

    // Stream methods
    @Override public InputStream getAsciiStream(int columnIndex) throws SQLException { return null; }
    @Override public InputStream getAsciiStream(String columnLabel) throws SQLException { return null; }
    @Override public InputStream getUnicodeStream(int columnIndex) throws SQLException { return null; }
    @Override public InputStream getUnicodeStream(String columnLabel) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(int columnIndex) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(String columnLabel) throws SQLException { return null; }
    @Override public Reader getCharacterStream(int columnIndex) throws SQLException { return null; }
    @Override public Reader getCharacterStream(String columnLabel) throws SQLException { return null; }
    @Override public Reader getNCharacterStream(int columnIndex) throws SQLException { return null; }
    @Override public Reader getNCharacterStream(String columnLabel) throws SQLException { return null; }

    // Navigation - only forward
    @Override public void beforeFirst() throws SQLException { currentRow = -1; }
    @Override public void afterLast() throws SQLException { currentRow = data.size(); }
    @Override public boolean first() throws SQLException { currentRow = 0; return !data.isEmpty(); }
    @Override public boolean last() throws SQLException { currentRow = data.size() - 1; return !data.isEmpty(); }
    @Override public boolean absolute(int row) throws SQLException { currentRow = row - 1; return currentRow >= 0 && currentRow < data.size(); }
    @Override public boolean relative(int rows) throws SQLException { return absolute(currentRow + 1 + rows); }
    @Override public boolean previous() throws SQLException { currentRow--; return currentRow >= 0; }

    // Unsupported update methods
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

    // Other methods
    @Override public SQLWarning getWarnings() throws SQLException { return null; }
    @Override public void clearWarnings() throws SQLException { }
    @Override public String getCursorName() throws SQLException { return null; }
    @Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException { return getObject(columnIndex); }
    @Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { return getObject(columnLabel); }
    @Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException { return null; }
    @Override public <T> T getObject(String columnLabel, Class<T> type) throws SQLException { return null; }
    @Override public Ref getRef(int columnIndex) throws SQLException { return null; }
    @Override public Ref getRef(String columnLabel) throws SQLException { return null; }
    @Override public Blob getBlob(int columnIndex) throws SQLException { return null; }
    @Override public Blob getBlob(String columnLabel) throws SQLException { return null; }
    @Override public Clob getClob(int columnIndex) throws SQLException { return null; }
    @Override public Clob getClob(String columnLabel) throws SQLException { return null; }
    @Override public Array getArray(int columnIndex) throws SQLException { return null; }
    @Override public Array getArray(String columnLabel) throws SQLException { return null; }
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
    @Override public Statement getStatement() throws SQLException { return null; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
