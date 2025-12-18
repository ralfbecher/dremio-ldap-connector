# Dremio ARP Connector for LDAP (OpenLDAP)

The LDAP connector allows Dremio to connect to and query data in LDAP directories using SQL. This enables you to build custom reports, dashboards, or run ad-hoc SQL queries against your LDAP directory via your client tool of choice.

This connector uses the **open-source [OpenLDAP JDBC-LDAP Bridge Driver](https://www.openldap.org/jdbcldap/)** - no commercial license required.

## Prerequisites

- Dremio 20.0.0 or later
- Java 8 or later
- OpenLDAP JDBC-LDAP Bridge Driver (see installation steps below)

## Configuration

Edit `pom.xml` and replace `{VERSION}` with your Dremio version, e.g., `25.1.1-202409260159070462-716c0676`

## Build and Installation

### Step 1: Build the OpenLDAP JDBC Driver

The OpenLDAP JDBC-LDAP driver is not available in Maven Central. You need to build it from source:

```bash
# Clone the repository
git clone https://github.com/zoesolutions/openldap-jdbcldap.git

# Build with Ant
cd openldap-jdbcldap
ant

# The JAR will be in the dist/ folder
```

Alternatively, use the official OpenLDAP source:
```bash
git clone https://git.openldap.org/openldap/jdbcldap.git
cd jdbcldap
ant
```

### Step 2: Build the Dremio Connector

```bash
mvn clean install
```

### Step 3: Deploy to Dremio

1. Copy the connector JAR (from `target/`) to Dremio's `/jars/` directory:
   ```bash
   docker cp target/dremio-ldaparp-plugin-{VERSION}.jar dremio:/opt/dremio/jars/
   ```

2. Copy the JDBC-LDAP driver JAR to Dremio's `/jars/3rdparty/` directory:
   ```bash
   docker cp openldap-jdbcldap/dist/jdbcLdap.jar dremio:/opt/dremio/jars/3rdparty/
   ```

3. Restart Dremio

## Usage

After installation, add a new LDAP source in Dremio with the following configuration:

| Field | Description | Example |
|-------|-------------|---------|
| LDAP Host | LDAP server hostname | `ldap.example.com` |
| Port | LDAP port (389 for LDAP, 636 for LDAPS) | `389` |
| Base DN | Base Distinguished Name for searches | `dc=example,dc=com` |
| Bind DN | User DN for authentication (optional) | `cn=admin,dc=example,dc=com` |
| Password | Password for authentication | |
| Use SSL | Enable SSL/TLS connection | `false` |
| Search Scope | `subTreeScope`, `oneLevelScope`, or `baseScope` | `subTreeScope` |

### Example Queries

```sql
-- List all users
SELECT * FROM ldap.Users

-- Find specific user
SELECT cn, mail, telephoneNumber
FROM ldap.Users
WHERE uid = 'jdoe'

-- Search by pattern
SELECT cn, mail
FROM ldap.Users
WHERE cn LIKE 'John%'
```

## Limitations

The OpenLDAP JDBC-LDAP driver has more limited SQL support compared to commercial drivers:

- **No JOINs pushed down** - Dremio handles joins locally
- **No aggregations pushed down** - COUNT, SUM, etc. are computed by Dremio
- **No subqueries** - Subqueries are processed by Dremio
- **Basic filtering** - Equality, comparison, LIKE, AND, OR operators are supported

These limitations don't affect functionality - Dremio automatically handles unsupported operations locally.

## ARP Overview

This connector uses the Advanced Relational Pushdown (ARP) Framework. The ARP configuration is in `src/main/resources/arp/implementation/ldap-arp.yaml` and defines:

- **Data type mappings** between LDAP and Dremio
- **Supported SQL operations** that can be pushed to the JDBC driver
- **Query syntax** customizations

## Resources

- [OpenLDAP JDBC-LDAP Documentation](https://www.openldap.org/jdbcldap/)
- [OpenLDAP JDBC-LDAP Source (GitHub)](https://github.com/zoesolutions/openldap-jdbcldap)
- [OpenLDAP JDBC-LDAP Source (Official)](https://git.openldap.org/openldap/jdbcldap)
- [Dremio ARP Framework Documentation](https://www.dremio.com/)

