package com.dremio.exec.store.jdbc.conf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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

        // Extract baseDN from URL (comes after host:port/)
        String baseDN = extractBaseDN(url);
        // Extract OBJECT_CLASSES, ATTRIBUTES, MAX_ROWS and USE_OBJECT_CATEGORY parameters from URL
        String objectClasses = extractParameter(url, "OBJECT_CLASSES");
        String attributes = extractParameter(url, "ATTRIBUTES");
        String maxRowsStr = extractParameter(url, "MAX_ROWS");
        String useObjectCategoryStr = extractParameter(url, "USE_OBJECT_CATEGORY");
        String skipFilterStr = extractParameter(url, "SKIP_FILTER");
        int maxRows = 500; // default
        if (maxRowsStr != null) {
            try {
                maxRows = Integer.parseInt(maxRowsStr);
            } catch (NumberFormatException e) {
                LOG.log(Level.WARNING, "Invalid MAX_ROWS value: " + maxRowsStr + ", using default 500");
            }
        }
        boolean useObjectCategory = "true".equalsIgnoreCase(useObjectCategoryStr);
        boolean skipFilter = "true".equalsIgnoreCase(skipFilterStr);

        // Log connection details (mask password for security)
        String maskedUrl = url.replaceAll("(SECURITY_CREDENTIALS:=)[^&]*", "$1****");
        LOG.log(Level.WARNING, "=== LDAP Driver Connect ===");
        LOG.log(Level.WARNING, "URL (masked): " + maskedUrl);
        LOG.log(Level.WARNING, "BaseDN: " + baseDN);
        LOG.log(Level.WARNING, "OBJECT_CLASSES: " + objectClasses);
        LOG.log(Level.WARNING, "ATTRIBUTES: " + attributes);
        LOG.log(Level.WARNING, "MAX_ROWS: " + maxRows);
        LOG.log(Level.WARNING, "USE_OBJECT_CATEGORY: " + useObjectCategory);
        LOG.log(Level.WARNING, "SKIP_FILTER: " + skipFilter);

        // URL-decode the credentials before passing to underlying driver
        // Both SECURITY_PRINCIPAL and SECURITY_CREDENTIALS may be URL-encoded
        String principal = extractParameter(url, "SECURITY_PRINCIPAL");
        String credentials = extractParameter(url, "SECURITY_CREDENTIALS");

        if (principal != null && (principal.contains("%") || principal.contains("+"))) {
            LOG.log(Level.WARNING, "Principal appears to be URL-encoded, decoding...");
            try {
                String decoded = URLDecoder.decode(principal, "UTF-8");
                url = url.replace("SECURITY_PRINCIPAL:=" + principal,
                                  "SECURITY_PRINCIPAL:=" + decoded);
                LOG.log(Level.WARNING, "Principal decoded: " + decoded);
            } catch (UnsupportedEncodingException e) {
                LOG.log(Level.WARNING, "Failed to decode principal: " + e.getMessage());
            }
        }

        if (credentials != null && (credentials.contains("%") || credentials.contains("+"))) {
            LOG.log(Level.WARNING, "Password appears to be URL-encoded, decoding...");
            try {
                String decoded = URLDecoder.decode(credentials, "UTF-8");
                url = url.replace("SECURITY_CREDENTIALS:=" + credentials,
                                  "SECURITY_CREDENTIALS:=" + decoded);
                LOG.log(Level.WARNING, "Password decoded successfully (length: " + decoded.length() + ")");
            } catch (UnsupportedEncodingException e) {
                LOG.log(Level.WARNING, "Failed to decode password: " + e.getMessage());
            }
        }

        // Log the final URL being passed to the LDAP driver (masked)
        String finalMaskedUrl = url.replaceAll("(SECURITY_CREDENTIALS:=)[^&]*", "$1****");
        LOG.log(Level.WARNING, "Final URL for LDAP driver: " + finalMaskedUrl);

        try {
            Connection rawConnection = delegate.connect(url, info);
            if (rawConnection == null) {
                LOG.log(Level.WARNING, "Delegate driver returned null connection - check LDAP URL and credentials");
                return null;
            }

            // Log connection success
            LOG.log(Level.WARNING, "LDAP connection established successfully");

            // Check for any connection warnings
            java.sql.SQLWarning warning = rawConnection.getWarnings();
            while (warning != null) {
                LOG.log(Level.WARNING, "Connection warning: " + warning.getMessage());
                warning = warning.getNextWarning();
            }

            LOG.log(Level.WARNING, "Created LdapConnection wrapper with baseDN: " + baseDN);
            return new LdapConnection(rawConnection, baseDN, objectClasses, attributes, maxRows, useObjectCategory, skipFilter, url);
        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "LDAP connection failed: " + e.getMessage());
            LOG.log(Level.SEVERE, "SQL State: " + e.getSQLState());
            LOG.log(Level.SEVERE, "Error Code: " + e.getErrorCode());
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Extract the base DN from the JDBC URL.
     * URL format: jdbc:ldap://host:port/baseDN?params
     */
    private String extractBaseDN(String url) {
        if (url == null) {
            return null;
        }
        // Find the position after host:port/
        int slashCount = 0;
        int startIndex = -1;
        for (int i = 0; i < url.length(); i++) {
            if (url.charAt(i) == '/') {
                slashCount++;
                if (slashCount == 3) {
                    startIndex = i + 1;
                    break;
                }
            }
        }
        if (startIndex == -1 || startIndex >= url.length()) {
            return null;
        }
        // Find the end (? or end of string)
        int endIndex = url.indexOf('?', startIndex);
        if (endIndex == -1) {
            endIndex = url.length();
        }
        return url.substring(startIndex, endIndex);
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
