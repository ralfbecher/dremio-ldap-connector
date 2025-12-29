package com.dremio.exec.store.jdbc.conf;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Statement wrapper that transforms SQL queries for the LDAP JDBC driver.
 *
 * The LDAP JDBC driver expects table names to be LDAP DNs, not objectClass names.
 * This wrapper transforms queries from:
 *   SELECT ... FROM person
 * to:
 *   SELECT ... FROM baseDN WHERE objectClass='person'
 */
public class LdapStatement implements Statement {
    private static final Logger LOG = Logger.getLogger(LdapStatement.class.getName());

    // Pattern to match FROM clause with table name (handles quoted and unquoted)
    private static final Pattern FROM_PATTERN = Pattern.compile(
        "\\bFROM\\s+\"?([\\w]+)\"?(?:\\s+(?:AS\\s+)?\"?([\\w]+)\"?)?",
        Pattern.CASE_INSENSITIVE
    );

    private final Statement delegate;
    private final String baseDN;
    private final String[] objectClasses;
    private final String[] attributes;
    private final int maxRows;
    private final boolean useObjectCategory;
    private final boolean skipFilter;

    public LdapStatement(Statement delegate, String baseDN, String[] objectClasses, String[] attributes, int maxRows, boolean useObjectCategory, boolean skipFilter) {
        this.delegate = delegate;
        this.baseDN = baseDN;
        this.objectClasses = objectClasses != null ? objectClasses : new String[0];
        this.attributes = attributes != null ? attributes : new String[0];
        this.maxRows = maxRows;
        this.useObjectCategory = useObjectCategory;
        this.skipFilter = skipFilter;
    }

    /**
     * Transform the SQL query to be compatible with the LDAP JDBC driver.
     * Replaces objectClass table names with the baseDN and adds objectClass filter.
     * Also transforms SELECT clause to SELECT * to avoid schema mismatch issues.
     */
    private String transformQuery(String sql) {
        if (sql == null) {
            return null;
        }

        LOG.log(Level.WARNING, "=== LdapStatement transformQuery ===");
        LOG.log(Level.WARNING, "Original SQL: " + sql);
        LOG.log(Level.WARNING, "BaseDN: " + baseDN);
        LOG.log(Level.WARNING, "ObjectClasses: " + String.join(",", objectClasses));

        Matcher matcher = FROM_PATTERN.matcher(sql);
        if (!matcher.find()) {
            LOG.log(Level.WARNING, "No FROM clause found in query");
            return sql;
        }

        String tableName = matcher.group(1);
        String alias = matcher.group(2);

        LOG.log(Level.WARNING, "Found table: " + tableName + ", alias: " + alias);

        // Check if the table name is one of our configured objectClasses
        boolean isObjectClass = false;
        String matchedObjectClass = null;
        for (String oc : objectClasses) {
            if (oc != null && oc.equalsIgnoreCase(tableName)) {
                isObjectClass = true;
                matchedObjectClass = oc;
                break;
            }
        }

        if (!isObjectClass) {
            LOG.log(Level.WARNING, "Table name '" + tableName + "' is not a configured objectClass, passing through");
            return sql;
        }

        // Replace the table name with baseDN and add objectClass filter
        String transformedSql = sql;

        // Replace column references like "person.dn" with just "dn"
        transformedSql = transformedSql.replaceAll("\\b" + Pattern.quote(tableName) + "\\.", "");
        if (alias != null) {
            transformedSql = transformedSql.replaceAll("\\b" + Pattern.quote(alias) + "\\.", "");
        }

        // Replace SELECT columns with SELECT * for LDAP driver compatibility
        // The JDBC-LDAP driver may not handle explicit column lists well
        Pattern selectPattern = Pattern.compile("^\\s*SELECT\\s+.+?\\s+FROM\\b", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher selectMatcher = selectPattern.matcher(transformedSql);
        if (selectMatcher.find() && !transformedSql.toLowerCase().contains("count(")) {
            String originalSelect = selectMatcher.group();
            transformedSql = selectMatcher.replaceFirst("SELECT * FROM");
            LOG.log(Level.WARNING, "Replaced SELECT columns with SELECT * for LDAP compatibility");
        }

        // Replace FROM tableName with FROM baseDN
        // NOTE: Do NOT quote the baseDN - the JDBC-LDAP driver uses the FROM value directly
        // as the LDAP search base. Quotes would be included in the search base and break the query.
        String fromReplacement = "FROM " + baseDN;
        transformedSql = FROM_PATTERN.matcher(transformedSql).replaceFirst(fromReplacement);

        // Add objectClass/objectCategory filter to the WHERE clause
        // This is critical for Active Directory to avoid referral errors ("Operations Error")
        // The JDBC-LDAP driver translates WHERE clauses to LDAP filters
        // For AD, objectCategory is often more reliable than objectClass
        if (skipFilter) {
            LOG.log(Level.WARNING, "Skip filter mode enabled - no objectClass/objectCategory filter added");
        } else {
            String filterAttribute = useObjectCategory ? "objectCategory" : "objectClass";
            String objectClassFilter = filterAttribute + "='" + matchedObjectClass + "'";
            LOG.log(Level.WARNING, "Using filter: " + objectClassFilter);

            // Skip adding filter if it's already in the SQL (user provided their own filter)
            boolean sqlHasFilter = transformedSql.toLowerCase().contains(filterAttribute.toLowerCase() + "=") ||
                                   transformedSql.toLowerCase().contains(filterAttribute.toLowerCase() + " =");

            if (!sqlHasFilter) {
                // Check if there's already a WHERE clause
                Pattern wherePattern = Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE);
                if (wherePattern.matcher(transformedSql).find()) {
                    // Append to existing WHERE clause with AND
                    transformedSql = wherePattern.matcher(transformedSql).replaceFirst("WHERE " + objectClassFilter + " AND ");
                } else {
                    // Check if there's an ORDER BY clause
                    Pattern orderByPattern = Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE);
                    Matcher orderByMatcher = orderByPattern.matcher(transformedSql);
                    if (orderByMatcher.find()) {
                        // Insert WHERE before ORDER BY
                        transformedSql = transformedSql.substring(0, orderByMatcher.start()) +
                                         "WHERE " + objectClassFilter + " " +
                                         transformedSql.substring(orderByMatcher.start());
                    } else {
                        // Append WHERE at the end
                        transformedSql = transformedSql + " WHERE " + objectClassFilter;
                    }
                }
            } else {
                LOG.log(Level.WARNING, "SQL already contains filter attribute, skipping auto-filter");
            }
        }

        LOG.log(Level.WARNING, "Transformed SQL: " + transformedSql);
        return transformedSql;
    }

    /**
     * Extract column names from SELECT clause.
     */
    private String[] extractSelectColumns(String sql) {
        if (sql == null) return new String[0];

        // Match SELECT ... FROM
        Pattern selectPattern = Pattern.compile(
            "^\\s*SELECT\\s+(.+?)\\s+FROM\\b",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        Matcher matcher = selectPattern.matcher(sql);
        if (!matcher.find()) {
            return new String[0];
        }

        String selectPart = matcher.group(1).trim();

        // Handle count(*) and other aggregates
        if (selectPart.toLowerCase().contains("count(")) {
            return new String[0]; // Don't wrap aggregate queries
        }

        // Handle SELECT *
        if (selectPart.equals("*")) {
            return attributes; // Return all attributes
        }

        // Split by comma and clean up
        String[] parts = selectPart.split(",");
        String[] columns = new String[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String col = parts[i].trim();
            // Remove table prefix (e.g., "person.givenName" -> "givenName")
            int dotIndex = col.lastIndexOf('.');
            if (dotIndex >= 0) {
                col = col.substring(dotIndex + 1);
            }
            // Remove quotes
            col = col.replace("\"", "").trim();
            columns[i] = col;
        }

        LOG.log(Level.WARNING, "Extracted SELECT columns: " + String.join(", ", columns));
        return columns;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        // Set max rows to avoid LDAP size limit errors
        if (maxRows > 0) {
            try {
                delegate.setMaxRows(maxRows);
                LOG.log(Level.WARNING, "Set maxRows to: " + maxRows);
            } catch (SQLException e) {
                LOG.log(Level.WARNING, "Could not set maxRows: " + e.getMessage());
            }
        }

        String transformedSql = transformQuery(sql);
        ResultSet delegateResult = delegate.executeQuery(transformedSql);

        // Extract the columns from the original SQL to normalize the ResultSet metadata
        String[] requestedColumns = extractSelectColumns(sql);
        if (requestedColumns.length > 0) {
            LOG.log(Level.WARNING, "Wrapping ResultSet with " + requestedColumns.length +
                    " requested columns: " + String.join(", ", requestedColumns));
            return new LdapResultSet(delegateResult, requestedColumns);
        }

        return delegateResult;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return delegate.executeUpdate(transformQuery(sql));
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return delegate.execute(transformQuery(sql));
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.executeUpdate(transformQuery(sql), autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return delegate.executeUpdate(transformQuery(sql), columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return delegate.executeUpdate(transformQuery(sql), columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.execute(transformQuery(sql), autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return delegate.execute(transformQuery(sql), columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return delegate.execute(transformQuery(sql), columnNames);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return delegate.executeLargeUpdate(transformQuery(sql));
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.executeLargeUpdate(transformQuery(sql), autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return delegate.executeLargeUpdate(transformQuery(sql), columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return delegate.executeLargeUpdate(transformQuery(sql), columnNames);
    }

    // Delegate all other methods

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return delegate.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        delegate.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return delegate.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        delegate.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        delegate.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return delegate.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        delegate.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        delegate.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        delegate.setCursorName(name);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return delegate.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return delegate.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return delegate.getMoreResults();
    }

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

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return delegate.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return delegate.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        delegate.addBatch(transformQuery(sql));
    }

    @Override
    public void clearBatch() throws SQLException {
        delegate.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return delegate.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return delegate.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return delegate.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return delegate.getGeneratedKeys();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        delegate.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return delegate.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        delegate.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return delegate.isCloseOnCompletion();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return delegate.getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        delegate.setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return delegate.getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return delegate.executeLargeBatch();
    }

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
}
