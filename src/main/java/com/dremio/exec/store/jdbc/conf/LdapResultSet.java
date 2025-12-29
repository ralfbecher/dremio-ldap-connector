package com.dremio.exec.store.jdbc.conf;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ResultSet wrapper that normalizes LDAP query results to match the expected schema.
 *
 * LDAP entries can have variable attributes, but Dremio expects a fixed schema.
 * This wrapper ensures that:
 * 1. All declared columns are always present in the same order
 * 2. Missing columns return NULL
 * 3. Extra columns from LDAP are ignored
 */
public class LdapResultSet implements ResultSet {
    private static final Logger LOG = Logger.getLogger(LdapResultSet.class.getName());

    private final ResultSet delegate;
    private final String[] expectedColumns;
    private final Map<String, Integer> expectedColumnMap; // column name -> 1-based index
    private final Map<Integer, Integer> indexMapping; // our index -> delegate index (or -1 if not present)
    private final LdapResultSetMetaData metaData;
    private boolean mappingInitialized = false;

    public LdapResultSet(ResultSet delegate, String[] expectedColumns) throws SQLException {
        this.delegate = delegate;
        this.expectedColumns = expectedColumns;
        this.expectedColumnMap = new HashMap<>();
        this.indexMapping = new HashMap<>();

        // Build expected column map (1-based indexing for JDBC)
        for (int i = 0; i < expectedColumns.length; i++) {
            expectedColumnMap.put(expectedColumns[i].toLowerCase(), i + 1);
        }

        // Create our custom metadata
        this.metaData = new LdapResultSetMetaData(expectedColumns);

        LOG.log(Level.WARNING, "LdapResultSet created with " + expectedColumns.length + " expected columns: " +
                String.join(", ", expectedColumns));
    }

    /**
     * Initialize the mapping between our expected columns and the delegate's actual columns.
     * Called lazily on first data access.
     */
    private void initializeMapping() throws SQLException {
        if (mappingInitialized) return;

        ResultSetMetaData delegateMeta = delegate.getMetaData();
        int delegateColumnCount = delegateMeta.getColumnCount();

        // Build a map of delegate column names to their indices and log them
        Map<String, Integer> delegateColumnMap = new HashMap<>();
        StringBuilder delegateColsLog = new StringBuilder();
        for (int i = 1; i <= delegateColumnCount; i++) {
            String colName = delegateMeta.getColumnName(i);
            String colTypeName = delegateMeta.getColumnTypeName(i);
            int colType = delegateMeta.getColumnType(i);
            if (colName != null) {
                delegateColumnMap.put(colName.toLowerCase(), i);
            }
            if (i > 1) delegateColsLog.append(", ");
            delegateColsLog.append(colName).append("(").append(colTypeName).append("/").append(colType).append(")");
        }

        LOG.log(Level.WARNING, "=== LDAP Driver ResultSet Columns ===");
        LOG.log(Level.WARNING, "Delegate ResultSet has " + delegateColumnCount + " columns: " + delegateColsLog);

        // Map our expected columns to delegate columns
        for (int i = 0; i < expectedColumns.length; i++) {
            int ourIndex = i + 1; // 1-based
            String colName = expectedColumns[i].toLowerCase();
            Integer delegateIndex = delegateColumnMap.get(colName);

            if (delegateIndex != null) {
                indexMapping.put(ourIndex, delegateIndex);
                LOG.log(Level.FINE, "Mapped column " + expectedColumns[i] + " (our index " + ourIndex +
                        ") to delegate index " + delegateIndex);
            } else {
                indexMapping.put(ourIndex, -1); // Column not present in delegate
                LOG.log(Level.FINE, "Column " + expectedColumns[i] + " not found in delegate, will return NULL");
            }
        }

        mappingInitialized = true;
    }

    /**
     * Get the delegate column index for our column index, or -1 if not present.
     */
    private int getDelegateIndex(int columnIndex) throws SQLException {
        initializeMapping();
        Integer delegateIndex = indexMapping.get(columnIndex);
        return delegateIndex != null ? delegateIndex : -1;
    }

    /**
     * Get the delegate column index for a column name, or -1 if not present.
     */
    private int getDelegateIndexByName(String columnLabel) throws SQLException {
        Integer ourIndex = expectedColumnMap.get(columnLabel.toLowerCase());
        if (ourIndex == null) {
            // Column not in our expected list - try delegate directly
            try {
                return delegate.findColumn(columnLabel);
            } catch (SQLException e) {
                return -1;
            }
        }
        return getDelegateIndex(ourIndex);
    }

    // Navigation methods - delegate directly
    @Override
    public boolean next() throws SQLException {
        boolean hasNext = delegate.next();
        if (hasNext && !mappingInitialized) {
            // Initialize mapping on first row
            initializeMapping();
        }
        LOG.log(Level.WARNING, "next() called, hasNext=" + hasNext);
        return hasNext;
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }

    // Metadata - return our normalized metadata
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Integer index = expectedColumnMap.get(columnLabel.toLowerCase());
        if (index != null) {
            return index;
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    // String getters - the most common for LDAP
    @Override
    public String getString(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getString(delegateIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getString(delegateIndex);
    }

    // Object getters
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getObject(delegateIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getObject(delegateIndex);
    }

    // Boolean getters
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return false;
        }
        return delegate.getBoolean(delegateIndex);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return false;
        }
        return delegate.getBoolean(delegateIndex);
    }

    // Byte getters
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getByte(delegateIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getByte(delegateIndex);
    }

    // Short getters
    @Override
    public short getShort(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getShort(delegateIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getShort(delegateIndex);
    }

    // Int getters
    @Override
    public int getInt(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getInt(delegateIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getInt(delegateIndex);
    }

    // Long getters
    @Override
    public long getLong(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getLong(delegateIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getLong(delegateIndex);
    }

    // Float getters
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getFloat(delegateIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getFloat(delegateIndex);
    }

    // Double getters
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getDouble(delegateIndex);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return 0;
        }
        return delegate.getDouble(delegateIndex);
    }

    // BigDecimal getters
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBigDecimal(delegateIndex, scale);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBigDecimal(delegateIndex, scale);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBigDecimal(delegateIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBigDecimal(delegateIndex);
    }

    // Bytes getters
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBytes(delegateIndex);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBytes(delegateIndex);
    }

    // Date/Time getters
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getDate(delegateIndex);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getDate(delegateIndex);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getDate(delegateIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getDate(delegateIndex, cal);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTime(delegateIndex);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTime(delegateIndex);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTime(delegateIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTime(delegateIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTimestamp(delegateIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTimestamp(delegateIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTimestamp(delegateIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getTimestamp(delegateIndex, cal);
    }

    // Stream getters
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getAsciiStream(delegateIndex);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getAsciiStream(delegateIndex);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getUnicodeStream(delegateIndex);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getUnicodeStream(delegateIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBinaryStream(delegateIndex);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBinaryStream(delegateIndex);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getCharacterStream(delegateIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getCharacterStream(delegateIndex);
    }

    // Warnings
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    // Cursor name
    @Override
    public String getCursorName() throws SQLException {
        return delegate.getCursorName();
    }

    // Positioning
    @Override
    public boolean isBeforeFirst() throws SQLException {
        return delegate.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return delegate.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return delegate.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return delegate.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        delegate.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        delegate.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return delegate.first();
    }

    @Override
    public boolean last() throws SQLException {
        return delegate.last();
    }

    @Override
    public int getRow() throws SQLException {
        return delegate.getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return delegate.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return delegate.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return delegate.previous();
    }

    // Fetch direction/size
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        delegate.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    // Result set type/concurrency
    @Override
    public int getType() throws SQLException {
        return delegate.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return delegate.getConcurrency();
    }

    // Row updates detection (read-only for LDAP)
    @Override
    public boolean rowUpdated() throws SQLException {
        return delegate.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return delegate.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return delegate.rowDeleted();
    }

    // Update methods - LDAP is read-only, but we need to implement them
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void refreshRow() throws SQLException {
        delegate.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        delegate.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return delegate.getStatement();
    }

    // Ref, Blob, Clob, Array getters
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getObject(delegateIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getRef(delegateIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBlob(delegateIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getClob(delegateIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getArray(delegateIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getObject(delegateIndex, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getRef(delegateIndex);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getBlob(delegateIndex);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getClob(delegateIndex);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getArray(delegateIndex);
    }

    // URL getter
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getURL(delegateIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getURL(delegateIndex);
    }

    // Update Ref, Blob, Clob, Array
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    // RowId
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getRowId(delegateIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getRowId(delegateIndex);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    // NString, NClob, SQLXML
    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getNClob(delegateIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getNClob(delegateIndex);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getSQLXML(delegateIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getSQLXML(delegateIndex);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getNString(delegateIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getNString(delegateIndex);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getNCharacterStream(delegateIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getNCharacterStream(delegateIndex);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP ResultSet is read-only");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        int delegateIndex = getDelegateIndex(columnIndex);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getObject(delegateIndex, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        int delegateIndex = getDelegateIndexByName(columnLabel);
        if (delegateIndex == -1) {
            return null;
        }
        return delegate.getObject(delegateIndex, type);
    }

    // Wrapper methods
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return true;
        }
        return delegate.isWrapperFor(iface);
    }

    /**
     * Inner class for normalized ResultSetMetaData.
     */
    private static class LdapResultSetMetaData implements ResultSetMetaData {
        private final String[] columns;

        public LdapResultSetMetaData(String[] columns) {
            this.columns = columns;
        }

        @Override
        public int getColumnCount() throws SQLException {
            return columns.length;
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
        public int getColumnDisplaySize(int column) throws SQLException {
            return 255;
        }

        @Override
        public String getColumnLabel(int column) throws SQLException {
            return columns[column - 1];
        }

        @Override
        public String getColumnName(int column) throws SQLException {
            return columns[column - 1];
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
            return java.sql.Types.VARCHAR;
        }

        @Override
        public String getColumnTypeName(int column) throws SQLException {
            return "VARCHAR";
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

        @Override
        public String getColumnClassName(int column) throws SQLException {
            return String.class.getName();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) {
                return iface.cast(this);
            }
            throw new SQLException("Cannot unwrap to " + iface.getName());
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return iface.isInstance(this);
        }
    }
}
