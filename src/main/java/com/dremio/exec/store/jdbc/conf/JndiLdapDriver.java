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
 * A pure JNDI-based JDBC Driver for LDAP.
 *
 * This driver uses Java's built-in JNDI LDAP support instead of the
 * Octetstring JDBC-LDAP driver. No external LDAP driver JAR is needed.
 *
 * URL format: jdbc:ldap://host:port/baseDN?params
 *
 * Parameters:
 * - SEARCH_SCOPE:= subTreeScope | oneLevelScope | baseScope
 * - SECURITY_PRINCIPAL:= bind DN (user)
 * - SECURITY_CREDENTIALS:= password
 * - OBJECT_CLASSES:= comma-separated list of objectClasses to expose as tables
 * - ATTRIBUTES:= comma-separated list of attributes to expose as columns
 * - MAX_ROWS:= maximum rows per query (LDAP size limit)
 * - USE_OBJECT_CATEGORY:= true/false (use objectCategory instead of objectClass for AD)
 * - SKIP_FILTER:= true/false (skip objectClass filter for debugging)
 */
public class JndiLdapDriver implements Driver {
    private static final Logger LOG = Logger.getLogger(JndiLdapDriver.class.getName());
    private static final String URL_PREFIX = "jdbc:ldap:";

    static {
        try {
            DriverManager.registerDriver(new JndiLdapDriver());
            LOG.log(Level.INFO, "JndiLdapDriver registered");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register JndiLdapDriver", e);
        }
    }

    public JndiLdapDriver() {
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        LOG.log(Level.WARNING, "=== JndiLdapDriver.connect ===");

        // Parse URL components
        String ldapUrl = extractLdapUrl(url);
        String baseDN = extractBaseDN(url);

        // Extract parameters
        String principal = extractParameter(url, "SECURITY_PRINCIPAL");
        String credentials = extractParameter(url, "SECURITY_CREDENTIALS");
        String objectClassesStr = extractParameter(url, "OBJECT_CLASSES");
        String attributesStr = extractParameter(url, "ATTRIBUTES");
        String maxRowsStr = extractParameter(url, "MAX_ROWS");
        String useObjectCategoryStr = extractParameter(url, "USE_OBJECT_CATEGORY");
        String skipFilterStr = extractParameter(url, "SKIP_FILTER");

        // URL-decode principal and credentials
        if (principal != null && (principal.contains("%") || principal.contains("+"))) {
            try {
                principal = URLDecoder.decode(principal, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                LOG.log(Level.WARNING, "Failed to decode principal: " + e.getMessage());
            }
        }
        if (credentials != null && (credentials.contains("%") || credentials.contains("+"))) {
            try {
                credentials = URLDecoder.decode(credentials, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                LOG.log(Level.WARNING, "Failed to decode credentials: " + e.getMessage());
            }
        }

        // Parse object classes
        String[] objectClasses;
        if (objectClassesStr != null && !objectClassesStr.isEmpty()) {
            objectClasses = objectClassesStr.split(",");
            for (int i = 0; i < objectClasses.length; i++) {
                objectClasses[i] = objectClasses[i].trim();
            }
        } else {
            objectClasses = new String[]{"person", "organizationalUnit", "group"};
        }

        // Parse attributes
        String[] attributes;
        if (attributesStr != null && !attributesStr.isEmpty()) {
            attributes = attributesStr.split(",");
            for (int i = 0; i < attributes.length; i++) {
                attributes[i] = attributes[i].trim();
            }
        } else {
            attributes = new String[]{"dn", "cn", "objectClass", "description"};
        }

        // Parse other settings
        int maxRows = 500;
        if (maxRowsStr != null) {
            try {
                maxRows = Integer.parseInt(maxRowsStr);
            } catch (NumberFormatException e) {
                LOG.log(Level.WARNING, "Invalid MAX_ROWS: " + maxRowsStr);
            }
        }

        boolean useObjectCategory = "true".equalsIgnoreCase(useObjectCategoryStr);
        boolean skipFilter = "true".equalsIgnoreCase(skipFilterStr);

        // Log connection details (mask password)
        LOG.log(Level.WARNING, "LDAP URL: " + ldapUrl);
        LOG.log(Level.WARNING, "Base DN: " + baseDN);
        LOG.log(Level.WARNING, "Principal: " + principal);
        LOG.log(Level.WARNING, "Credentials length: " + (credentials != null ? credentials.length() : 0));
        LOG.log(Level.WARNING, "Object classes: " + String.join(", ", objectClasses));
        LOG.log(Level.WARNING, "Attributes: " + String.join(", ", attributes));
        LOG.log(Level.WARNING, "Max rows: " + maxRows);
        LOG.log(Level.WARNING, "Use objectCategory: " + useObjectCategory);
        LOG.log(Level.WARNING, "Skip filter: " + skipFilter);

        // Create JNDI-based connection
        JndiLdapConnection connection = new JndiLdapConnection(
                ldapUrl, baseDN, principal, credentials,
                objectClasses, attributes, maxRows,
                useObjectCategory, skipFilter);

        LOG.log(Level.WARNING, "JndiLdapConnection created successfully");
        return connection;
    }

    /**
     * Extract LDAP URL (protocol://host:port) from JDBC URL.
     */
    private String extractLdapUrl(String url) {
        // jdbc:ldap://host:port/baseDN?params
        if (!url.startsWith("jdbc:ldap://")) {
            return null;
        }
        String temp = url.substring(5); // Remove "jdbc:"
        int slashPos = temp.indexOf('/', 7); // After "ldap://"
        if (slashPos > 0) {
            return temp.substring(0, slashPos);
        }
        int queryPos = temp.indexOf('?');
        if (queryPos > 0) {
            return temp.substring(0, queryPos);
        }
        return temp;
    }

    /**
     * Extract base DN from JDBC URL.
     */
    private String extractBaseDN(String url) {
        // Find after third slash (jdbc:ldap://host:port/baseDN?params)
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
            return "";
        }
        int endIndex = url.indexOf('?', startIndex);
        if (endIndex == -1) {
            endIndex = url.length();
        }
        return url.substring(startIndex, endIndex);
    }

    /**
     * Extract a parameter value from the JDBC URL.
     */
    private String extractParameter(String url, String paramName) {
        String searchPattern = "&" + paramName + ":=";
        int startIndex = url.indexOf(searchPattern);
        if (startIndex == -1) {
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
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOG;
    }
}
