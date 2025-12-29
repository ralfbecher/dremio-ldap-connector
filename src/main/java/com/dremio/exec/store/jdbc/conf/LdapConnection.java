package com.dremio.exec.store.jdbc.conf;

import java.sql.*;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

/**
 * A Connection wrapper that silently ignores transaction-related operations.
 *
 * LDAP does not support transactions, but DBCP2's connection pool calls
 * setAutoCommit(), commit(), and rollback() during connection lifecycle
 * management (activation, passivation, validation). This wrapper prevents
 * those calls from reaching the underlying LDAP JDBC driver, which would
 * throw "LDAP Does Not Support Transactions" exceptions.
 */
public class LdapConnection implements Connection {
    private static final Logger LOG = Logger.getLogger(LdapConnection.class.getName());

    private final Connection delegate;
    private final String baseDN;
    private final String[] objectClasses;
    private final String[] attributes;
    private final int maxRows;
    private final boolean useObjectCategory;
    private final boolean skipFilter;
    private final String originalUrl;  // Store original URL with all parameters
    private final String ldapUrl;      // LDAP URL (host:port)
    private final String principal;    // Bind DN
    private final String credentials;  // Password

    public LdapConnection(Connection delegate, String baseDN, String objectClassesParam, String attributesParam, int maxRows, boolean useObjectCategory, boolean skipFilter, String originalUrl) {
        this.delegate = delegate;
        this.baseDN = baseDN != null ? baseDN : "";
        this.maxRows = maxRows;
        this.useObjectCategory = useObjectCategory;
        this.skipFilter = skipFilter;
        this.originalUrl = originalUrl;

        // Extract LDAP connection details from URL for JNDI-based statement
        String extractedLdapUrl = null;
        String extractedPrincipal = null;
        String extractedCredentials = null;

        if (originalUrl != null && originalUrl.startsWith("jdbc:ldap://")) {
            String temp = originalUrl.replace("jdbc:ldap://", "ldap://");
            int queryStart = temp.indexOf('?');
            if (queryStart > 0) {
                // Extract host:port
                String hostPart = temp.substring(0, queryStart);
                int slashPos = hostPart.indexOf('/', 7);
                if (slashPos > 0) {
                    extractedLdapUrl = hostPart.substring(0, slashPos);
                } else {
                    extractedLdapUrl = hostPart;
                }

                // Extract credentials from query params
                String params = temp.substring(queryStart + 1);
                for (String param : params.split("&")) {
                    if (param.startsWith("SECURITY_PRINCIPAL:=")) {
                        extractedPrincipal = param.substring(20);
                    } else if (param.startsWith("SECURITY_CREDENTIALS:=")) {
                        extractedCredentials = param.substring(22);
                    }
                }
            }
        }

        this.ldapUrl = extractedLdapUrl;
        this.principal = extractedPrincipal;
        this.credentials = extractedCredentials;
        // Parse object classes from the parameter
        if (objectClassesParam != null && !objectClassesParam.isEmpty()) {
            this.objectClasses = objectClassesParam.split(",");
            for (int i = 0; i < this.objectClasses.length; i++) {
                this.objectClasses[i] = this.objectClasses[i].trim();
            }
        } else {
            this.objectClasses = new String[0];
        }
        // Parse attributes from the parameter
        if (attributesParam != null && !attributesParam.isEmpty()) {
            this.attributes = attributesParam.split(",");
            for (int i = 0; i < this.attributes.length; i++) {
                this.attributes[i] = this.attributes[i].trim();
            }
        } else {
            this.attributes = new String[0];
        }
        // Test query to verify LDAP connection works
        testLdapQuery();
    }

    /**
     * Execute a test query to verify the LDAP driver can retrieve data.
     */
    private void testLdapQuery() {
        try {
            LOG.log(Level.WARNING, "=== Testing LDAP Connection ===");
            LOG.log(Level.WARNING, "BaseDN: " + baseDN);
            LOG.log(Level.WARNING, "ObjectClasses: " + (objectClasses != null ? String.join(",", objectClasses) : "none"));
            LOG.log(Level.WARNING, "Attributes: " + (attributes != null ? String.join(",", attributes) : "none"));

            // First, try to get metadata to see if connection works
            try {
                DatabaseMetaData dbMeta = delegate.getMetaData();
                LOG.log(Level.WARNING, "Driver name: " + dbMeta.getDriverName());
                LOG.log(Level.WARNING, "Driver version: " + dbMeta.getDriverVersion());
                LOG.log(Level.WARNING, "Database product: " + dbMeta.getDatabaseProductName());
                LOG.log(Level.WARNING, "Database version: " + dbMeta.getDatabaseProductVersion());
                LOG.log(Level.WARNING, "User name: " + dbMeta.getUserName());
                LOG.log(Level.WARNING, "URL: " + dbMeta.getURL());
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Could not get database metadata: " + e.getMessage());
            }

            // Try multiple query variations to diagnose the issue
            // The JDBC-LDAP driver may require specific syntax:
            // - Explicit attribute names (not SELECT *)
            // - LDAP-style filters (without quotes, or with parentheses)
            String[] testQueries = {
                // Try with explicit common attributes
                "SELECT dn, cn, objectClass FROM " + baseDN,
                // Try LDAP filter without quotes
                "SELECT dn, cn FROM " + baseDN + " WHERE objectClass=*",
                // Try with parentheses (LDAP filter style)
                "SELECT dn, cn FROM " + baseDN + " WHERE (objectClass=*)",
                // Try original SELECT *
                "SELECT * FROM " + baseDN,
                // Try with quoted baseDN (some drivers need this)
                "SELECT dn, cn FROM \"" + baseDN + "\"",
                // Try scope specification in query
                "SELECT dn, cn FROM " + baseDN + " SCOPE subtree",
                // Try root DSE query (should always work if connected)
                "SELECT * FROM RootDSE",
                // Try with LDAP URL style baseDN
                "SELECT dn FROM o=" + baseDN
            };

            for (String testSql : testQueries) {
                try {
                    LOG.log(Level.WARNING, "--- Test Query ---");
                    LOG.log(Level.WARNING, "SQL: " + testSql);

                    try (Statement stmt = delegate.createStatement()) {
                        stmt.setMaxRows(5); // Limit to 5 rows for testing
                        LOG.log(Level.WARNING, "Statement created, executing query...");

                        try (ResultSet rs = stmt.executeQuery(testSql)) {
                            LOG.log(Level.WARNING, "Query executed, ResultSet class: " + rs.getClass().getName());

                            ResultSetMetaData meta = rs.getMetaData();
                            int colCount = meta.getColumnCount();

                            // Log columns from the result
                            StringBuilder cols = new StringBuilder();
                            for (int i = 1; i <= colCount; i++) {
                                if (i > 1) cols.append(", ");
                                cols.append(meta.getColumnName(i)).append("(").append(meta.getColumnTypeName(i)).append(")");
                            }
                            LOG.log(Level.WARNING, "Columns (" + colCount + "): " + cols);

                            // Count rows and log first few
                            int rowCount = 0;
                            while (rs.next()) {
                                rowCount++;
                                if (rowCount <= 3 && colCount > 0) {
                                    StringBuilder row = new StringBuilder();
                                    for (int i = 1; i <= Math.min(colCount, 5); i++) {
                                        if (i > 1) row.append(", ");
                                        try {
                                            String val = rs.getString(i);
                                            row.append(meta.getColumnName(i)).append("=").append(
                                                val != null ? (val.length() > 50 ? val.substring(0, 50) + "..." : val) : "null");
                                        } catch (Exception e) {
                                            row.append(meta.getColumnName(i)).append("=ERROR:" + e.getMessage());
                                        }
                                    }
                                    LOG.log(Level.WARNING, "Row " + rowCount + ": " + row);
                                }
                            }
                            LOG.log(Level.WARNING, "Total rows: " + rowCount);

                            // If we got data, we're done testing
                            if (rowCount > 0) {
                                LOG.log(Level.WARNING, "=== Test successful! Data retrieved ===");
                                return;
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Query failed: " + e.getClass().getName() + ": " + e.getMessage());
                    // Get more details from SQLException
                    if (e instanceof SQLException) {
                        SQLException se = (SQLException) e;
                        LOG.log(Level.WARNING, "  SQL State: " + se.getSQLState());
                        LOG.log(Level.WARNING, "  Error Code: " + se.getErrorCode());
                        Throwable cause = se.getCause();
                        while (cause != null) {
                            LOG.log(Level.WARNING, "  Caused by: " + cause.getClass().getName() + ": " + cause.getMessage());
                            cause = cause.getCause();
                        }
                    }
                    e.printStackTrace();
                }
            }

            LOG.log(Level.WARNING, "=== All JDBC test queries returned 0 rows ===");

            // Try direct JNDI LDAP to see if the issue is with JDBC-LDAP driver
            testDirectJndi();

        } catch (Exception e) {
            LOG.log(Level.WARNING, "Test query setup failed: " + e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Test LDAP connection directly using JNDI to bypass the JDBC-LDAP driver.
     * This helps determine if the issue is with the driver or the LDAP connection itself.
     */
    private void testDirectJndi() {
        LOG.log(Level.WARNING, "=== Testing Direct JNDI LDAP ===");
        try {
            // Use the original URL passed from LdapDriver (contains all parameters including credentials)
            String url = originalUrl;
            if (url == null) {
                url = delegate.getMetaData().getURL();
            }

            String maskedUrl = url != null ? url.replaceAll("(SECURITY_CREDENTIALS:=)[^&]*", "$1****") : "null";
            LOG.log(Level.WARNING, "Using URL (masked): " + maskedUrl);

            // Parse the URL to extract LDAP connection details
            // Format: jdbc:ldap://host:port/baseDN?params
            if (url == null || !url.startsWith("jdbc:ldap://")) {
                LOG.log(Level.WARNING, "Cannot parse JDBC URL for JNDI test");
                return;
            }

            String ldapUrl = url.replace("jdbc:ldap://", "ldap://");
            int queryStart = ldapUrl.indexOf('?');
            String ldapHost;
            String params = "";
            if (queryStart > 0) {
                ldapHost = ldapUrl.substring(0, queryStart);
                params = ldapUrl.substring(queryStart + 1);
            } else {
                ldapHost = ldapUrl;
            }

            // Extract just host:port
            int slashPos = ldapHost.indexOf('/', 7); // After "ldap://"
            if (slashPos > 0) {
                ldapHost = ldapHost.substring(0, slashPos);
            }

            LOG.log(Level.WARNING, "LDAP URL for JNDI: " + ldapHost);
            LOG.log(Level.WARNING, "BaseDN: " + baseDN);

            // Extract credentials from params (they should already be decoded by LdapDriver)
            String principal = null;
            String credentials = null;
            for (String param : params.split("&")) {
                if (param.startsWith("SECURITY_PRINCIPAL:=")) {
                    principal = param.substring(20);  // Already decoded by LdapDriver
                } else if (param.startsWith("SECURITY_CREDENTIALS:=")) {
                    credentials = param.substring(22);  // Already decoded by LdapDriver
                }
            }

            LOG.log(Level.WARNING, "Principal: " + principal);
            LOG.log(Level.WARNING, "Credentials length: " + (credentials != null ? credentials.length() : 0));

            // Set up JNDI environment
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, ldapHost + "/" + baseDN);

            if (principal != null && !principal.isEmpty()) {
                env.put(Context.SECURITY_AUTHENTICATION, "simple");
                env.put(Context.SECURITY_PRINCIPAL, principal);
                env.put(Context.SECURITY_CREDENTIALS, credentials != null ? credentials : "");
            }

            // Disable referral following
            env.put(Context.REFERRAL, "ignore");

            LOG.log(Level.WARNING, "Creating JNDI context...");
            DirContext ctx = new InitialDirContext(env);
            LOG.log(Level.WARNING, "JNDI context created successfully!");

            // Try a simple search
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            searchControls.setCountLimit(5);
            searchControls.setReturningAttributes(new String[]{"dn", "cn", "objectClass"});

            LOG.log(Level.WARNING, "Executing JNDI search with filter (objectClass=*)...");
            NamingEnumeration<SearchResult> results = ctx.search("", "(objectClass=*)", searchControls);

            int count = 0;
            while (results.hasMore()) {
                SearchResult sr = results.next();
                count++;
                if (count <= 3) {
                    LOG.log(Level.WARNING, "JNDI Result " + count + ": " + sr.getNameInNamespace());
                    Attributes attrs = sr.getAttributes();
                    if (attrs != null) {
                        NamingEnumeration<?> attrEnum = attrs.getAll();
                        while (attrEnum.hasMore()) {
                            LOG.log(Level.WARNING, "  Attr: " + attrEnum.next());
                        }
                    }
                }
            }
            LOG.log(Level.WARNING, "JNDI search returned " + count + " results");

            ctx.close();
            LOG.log(Level.WARNING, "=== JNDI test complete ===");

        } catch (Exception e) {
            LOG.log(Level.WARNING, "JNDI test failed: " + e.getClass().getName() + ": " + e.getMessage());
            Throwable cause = e.getCause();
            while (cause != null) {
                LOG.log(Level.WARNING, "  Caused by: " + cause.getClass().getName() + ": " + cause.getMessage());
                cause = cause.getCause();
            }
            e.printStackTrace();
        }
    }

    /**
     * Get the LDAP base DN.
     */
    public String getBaseDN() {
        return baseDN;
    }

    /**
     * Get the configured LDAP object classes to expose as tables.
     */
    public String[] getObjectClasses() {
        return objectClasses;
    }

    /**
     * Get the configured LDAP attributes to expose as columns.
     */
    public String[] getAttributes() {
        return attributes;
    }

    // Transaction methods - silently ignore
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // Silently ignore - LDAP doesn't support transactions
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        // Always report as auto-commit mode
        return true;
    }

    @Override
    public void commit() throws SQLException {
        // Silently ignore - LDAP doesn't support transactions
    }

    @Override
    public void rollback() throws SQLException {
        // Silently ignore - LDAP doesn't support transactions
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        // Silently ignore - LDAP doesn't support transactions
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP does not support savepoints");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP does not support savepoints");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP does not support savepoints");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // Silently ignore - LDAP doesn't support transaction isolation
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    // Delegate all other methods to the underlying connection

    @Override
    public Statement createStatement() throws SQLException {
        // Use JNDI-based statement that bypasses the broken Octetstring JDBC-LDAP driver
        if (ldapUrl != null) {
            return new JndiLdapStatement(this, ldapUrl, baseDN, principal, credentials,
                    objectClasses, attributes, maxRows, useObjectCategory, skipFilter);
        }
        // Fall back to old LdapStatement if URL parsing failed
        return new LdapStatement(delegate.createStatement(), baseDN, objectClasses, attributes, maxRows, useObjectCategory, skipFilter);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return delegate.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return delegate.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return delegate.nativeSQL(sql);
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        // Wrap the metadata to handle null returns from the LDAP driver
        // and to expose configured object classes as tables and attributes as columns
        return new LdapDatabaseMetaData(delegate.getMetaData(), this, objectClasses, attributes);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        delegate.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return delegate.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        delegate.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return delegate.getCatalog();
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        // Use JNDI-based statement
        if (ldapUrl != null) {
            return new JndiLdapStatement(this, ldapUrl, baseDN, principal, credentials,
                    objectClasses, attributes, maxRows, useObjectCategory, skipFilter);
        }
        return new LdapStatement(delegate.createStatement(resultSetType, resultSetConcurrency), baseDN, objectClasses, attributes, maxRows, useObjectCategory, skipFilter);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return delegate.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        delegate.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        delegate.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        // Use JNDI-based statement
        if (ldapUrl != null) {
            return new JndiLdapStatement(this, ldapUrl, baseDN, principal, credentials,
                    objectClasses, attributes, maxRows, useObjectCategory, skipFilter);
        }
        return new LdapStatement(delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), baseDN, objectClasses, attributes, maxRows, useObjectCategory, skipFilter);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return delegate.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return delegate.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return delegate.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return delegate.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return delegate.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return delegate.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return delegate.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        delegate.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        delegate.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return delegate.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return delegate.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return delegate.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return delegate.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        delegate.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return delegate.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        delegate.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        delegate.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return delegate.getNetworkTimeout();
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
