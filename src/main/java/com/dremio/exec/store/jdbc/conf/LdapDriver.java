package com.dremio.exec.store.jdbc.conf;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A JDBC Driver wrapper that wraps the LDAP JDBC driver.
 *
 * This driver delegates to the underlying LDAP JDBC driver but returns
 * LdapConnection instances that silently ignore transaction-related operations.
 * This is necessary because DBCP2 calls setAutoCommit() during pool initialization
 * and validation, which happens before any DataSource-level wrapping can occur.
 *
 * By wrapping at the driver level, all connections are wrapped from the moment
 * they are created, preventing DBCP2 from calling setAutoCommit() on raw
 * LDAP connections.
 */
public class LdapDriver implements Driver {
    private static final Logger LOG = Logger.getLogger(LdapDriver.class.getName());
    private static final String LDAP_DRIVER_CLASS = "com.octetstring.jdbcLdap.sql.JdbcLdapDriver";
    private static final String URL_PREFIX = "jdbc:ldap:";

    private final Driver delegate;

    static {
        try {
            DriverManager.registerDriver(new LdapDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register LdapDriver", e);
        }
    }

    public LdapDriver() throws SQLException {
        try {
            Class<?> driverClass = Class.forName(LDAP_DRIVER_CLASS);
            this.delegate = (Driver) driverClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SQLException("Failed to load LDAP JDBC driver: " + LDAP_DRIVER_CLASS, e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        // Extract OBJECT_CLASSES and ATTRIBUTES parameters from URL
        String objectClasses = extractParameter(url, "OBJECT_CLASSES");
        String attributes = extractParameter(url, "ATTRIBUTES");

        LOG.log(Level.INFO, "LDAP Driver connecting with URL: " + url);
        LOG.log(Level.INFO, "Extracted OBJECT_CLASSES: " + objectClasses);
        LOG.log(Level.INFO, "Extracted ATTRIBUTES: " + attributes);

        Connection rawConnection = delegate.connect(url, info);
        if (rawConnection == null) {
            LOG.log(Level.WARNING, "Delegate driver returned null connection");
            return null;
        }

        LOG.log(Level.INFO, "Created LdapConnection wrapper");
        return new LdapConnection(rawConnection, objectClasses, attributes);
    }

    /**
     * Extract a parameter value from the JDBC URL.
     * Parameters are in format: &PARAM_NAME:=value
     */
    private String extractParameter(String url, String paramName) {
        String searchPattern = "&" + paramName + ":=";
        int startIndex = url.indexOf(searchPattern);
        if (startIndex == -1) {
            // Also try with ? prefix for first parameter
            searchPattern = "?" + paramName + ":=";
            startIndex = url.indexOf(searchPattern);
        }
        if (startIndex == -1) {
            return null;
        }
        startIndex += searchPattern.length();
        int endIndex = url.indexOf("&", startIndex);
        if (endIndex == -1) {
            endIndex = url.length();
        }
        return url.substring(startIndex, endIndex);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return delegate.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }
}
