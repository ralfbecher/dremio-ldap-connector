package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.store.jdbc.*;
import com.dremio.options.OptionManager;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

/**
 * Configuration for the OpenLDAP JDBC-LDAP Bridge connector.
 * Uses the open-source JDBC-LDAP driver from OpenLDAP.
 * @see <a href="https://www.openldap.org/jdbcldap/">OpenLDAP JDBC-LDAP</a>
 */
@SourceType(value = "LDAPARP", label = "LDAP", uiConfig = "ldap-layout.json")
public class LDAPConf extends AbstractArpConf<LDAPConf> {
  private static final String ARP_FILENAME = "arp/implementation/ldap-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  // Use our pure JNDI-based LDAP driver.
  // No external jdbcLdap.jar is needed - uses Java's built-in JNDI LDAP support.
  private static final String DRIVER = "com.dremio.exec.store.jdbc.conf.JndiLdapDriver";

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "LDAP Host")
  public String host;

  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 389;

  @NotBlank
  @Tag(3)
  @DisplayMetadata(label = "Base DN")
  public String baseDN;

  @Tag(4)
  @DisplayMetadata(label = "Bind DN (User)")
  public String bindDN = "";

  @Tag(5)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password = "";

  @Tag(6)
  @DisplayMetadata(label = "Use SSL")
  @NotMetadataImpacting
  public boolean useSSL = false;

  @Tag(7)
  @DisplayMetadata(label = "Search Scope")
  @NotMetadataImpacting
  public String searchScope = "subTreeScope";

  @Tag(8)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 200;

  @Tag(9)
  @DisplayMetadata(label = "Maximum idle connections")
  @NotMetadataImpacting
  public int maxIdleConns = 8;

  @Tag(10)
  @DisplayMetadata(label = "Connection idle time (s)")
  @NotMetadataImpacting
  public int idleTimeSec = 60;

  @Tag(11)
  @DisplayMetadata(label = "Object Classes (comma-separated)")
  @NotMetadataImpacting
  public String objectClasses = "user,group,computer,organizationalUnit,contact";

  @Tag(12)
  @DisplayMetadata(label = "Attributes (comma-separated)")
  @NotMetadataImpacting
  public String attributes = "dn,cn,objectClass,sAMAccountName,displayName,mail,givenName,sn,memberOf,member,description,userPrincipalName,distinguishedName";

  @Tag(13)
  @DisplayMetadata(label = "Max rows per query (LDAP size limit)")
  @NotMetadataImpacting
  public int maxRows = 500;

  @Tag(14)
  @DisplayMetadata(label = "Use objectCategory filter (for Active Directory)")
  @NotMetadataImpacting
  public boolean useObjectCategory = false;

  @Tag(15)
  @DisplayMetadata(label = "Skip objectClass/objectCategory filter (debug mode)")
  @NotMetadataImpacting
  public boolean skipFilter = false;

  /**
   * URL-encode a string for safe inclusion in JDBC URL parameters.
   * This is critical for passwords containing special characters like &, =, £, etc.
   */
  private String urlEncode(String value) {
    if (value == null) {
      return "";
    }
    try {
      return URLEncoder.encode(value, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // UTF-8 is always supported, this should never happen
      return value;
    }
  }

  @VisibleForTesting
  public String toJdbcConnectionString() {
    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:ldap://");
    sb.append(host);
    sb.append(":");
    sb.append(port);
    sb.append("/");
    sb.append(baseDN);
    sb.append("?SEARCH_SCOPE:=").append(searchScope);

    if (useSSL) {
      sb.append("&SECURITY_PROTOCOL:=ssl");
    }

    if (bindDN != null && !bindDN.isEmpty()) {
      sb.append("&SECURITY_PRINCIPAL:=").append(urlEncode(bindDN));
    }

    if (password != null && !password.isEmpty()) {
      sb.append("&SECURITY_CREDENTIALS:=").append(urlEncode(password));
    }

    // Pass object classes as a custom parameter for schema discovery
    if (objectClasses != null && !objectClasses.isEmpty()) {
      sb.append("&OBJECT_CLASSES:=").append(objectClasses);
    }

    // Pass attributes as a custom parameter for column discovery
    if (attributes != null && !attributes.isEmpty()) {
      sb.append("&ATTRIBUTES:=").append(attributes);
    }

    // Pass max rows for LDAP size limit
    sb.append("&MAX_ROWS:=").append(maxRows);

    // Pass objectCategory flag for Active Directory
    sb.append("&USE_OBJECT_CATEGORY:=").append(useObjectCategory);

    // Pass skipFilter flag for debugging
    sb.append("&SKIP_FILTER:=").append(skipFilter);

    return sb.toString();
  }

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(JdbcPluginConfig.Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
         return configBuilder.withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        //.addHiddenSchema("SYSTEM")
        .build();
  }

  private CloseableDataSource newDataSource() {
      // LdapDriver returns LdapConnection instances that silently ignore
      // transaction-related calls (setAutoCommit, commit, rollback).
      // This prevents DBCP2 from failing during pool initialization and validation.
      return DataSources.newGenericConnectionPoolDataSource(DRIVER,
          toJdbcConnectionString(), "", "", null, DataSources.CommitMode.FORCE_AUTO_COMMIT_MODE, maxIdleConns, idleTimeSec);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}

