# Dremio ARP Connector for LDAP / Active Directory

A Dremio connector that enables SQL queries against LDAP directories and Active Directory using Java's built-in JNDI (Java Naming and Directory Interface). No external LDAP driver required.

## Features

- **Pure JNDI Implementation** - Uses Java's built-in LDAP support, no external jdbcLdap.jar needed
- **Active Directory Optimized** - Default configuration tuned for Microsoft Active Directory
- **SQL to LDAP Translation** - Automatically converts SQL WHERE clauses to LDAP filter syntax
- **Schema Discovery** - Exposes LDAP objectClasses as tables and attributes as columns
- **Flexible Filtering** - Supports objectClass or objectCategory filtering (AD compatibility)

## Prerequisites

- Dremio 20.0.0 or later
- Java 8 or later

## Build and Installation

### Step 1: Configure Dremio Version (Optional)

The connector is pre-configured for Dremio 25.2.0. To use a different version, edit `pom.xml`:

```xml
<dremio.version>25.2.0-202502041324311032-f9bf85a8</dremio.version>
```

### Step 2: Build the Connector

```bash
mvn clean install
```

### Step 3: Deploy to Dremio

Copy the connector JAR to Dremio's `/jars/` directory:

```bash
# For Docker deployment
docker cp target/dremio-ldap-plugin-25.2.0.jar dremio:/opt/dremio/jars/

# Restart Dremio
docker restart dremio
```

**Note:** No additional driver JAR is needed - the connector uses Java's built-in JNDI LDAP support.

## Configuration

After installation, add a new "LDAP" source in Dremio:

### Connection Settings

| Field | Description | Example |
|-------|-------------|---------|
| LDAP Host | LDAP/AD server hostname | `dc01.example.com` |
| Port | LDAP port | `389` (LDAP), `636` (LDAPS), `3268` (AD Global Catalog) |
| Base DN | Base Distinguished Name | `DC=example,DC=com` |
| Bind DN (User) | User DN for authentication | `CN=svc_dremio,OU=Service,DC=example,DC=com` |
| Password | Password for authentication | |
| Use SSL | Enable SSL/TLS | `true` for port 636 |

### Active Directory Recommendations

| Setting | Recommended Value | Notes |
|---------|-------------------|-------|
| Port | `3268` | Global Catalog - queries entire forest |
| Use objectCategory filter | `true` | More efficient for AD queries |
| Skip objectClass filter | `false` | Enable filtering by object type |

### Schema Settings (Defaults optimized for AD)

| Field | Default Value |
|-------|---------------|
| Object Classes | `user,group,computer,organizationalUnit,contact` |
| Attributes | `dn,cn,objectClass,sAMAccountName,displayName,mail,givenName,sn,memberOf,member,description,userPrincipalName,distinguishedName` |

These become your tables and columns in Dremio.

#### Default Object Classes (Tables)

| Object Class | Description |
|--------------|-------------|
| `user` | User accounts |
| `group` | Security and distribution groups |
| `computer` | Computer accounts |
| `organizationalUnit` | Organizational Units (OUs) |
| `contact` | Contact objects |

#### Default Attributes (Columns)

| Attribute | Description |
|-----------|-------------|
| `dn` | Distinguished Name (unique identifier) |
| `cn` | Common Name |
| `objectClass` | Object type |
| `sAMAccountName` | Windows login name (pre-Windows 2000) |
| `displayName` | Full display name |
| `mail` | Email address |
| `givenName` | First name |
| `sn` | Surname (last name) |
| `memberOf` | Groups the object belongs to |
| `member` | Members of a group |
| `description` | Description field |
| `userPrincipalName` | UPN format (user@domain.com) |
| `distinguishedName` | Full DN path |

## Usage

### Available Tables

With default configuration, you get these tables:

| Table | Description |
|-------|-------------|
| `user` | User accounts |
| `group` | Security and distribution groups |
| `computer` | Computer accounts |
| `organizationalUnit` | Organizational Units |
| `contact` | Contact objects |

### Example Queries

```sql
-- List all users
SELECT * FROM "LDAP"."user"

-- Find users by name pattern
SELECT displayName, mail, sAMAccountName
FROM "LDAP"."user"
WHERE cn LIKE 'John%'

-- Get all groups
SELECT cn, description, member
FROM "LDAP"."group"

-- Find users in a specific OU (using distinguishedName)
SELECT displayName, mail
FROM "LDAP"."user"
WHERE distinguishedName LIKE '%OU=Sales%'

-- Find group members
SELECT cn, member
FROM "LDAP"."group"
WHERE cn = 'IT-Admins'

-- Find computers
SELECT cn, description
FROM "LDAP"."computer"
```

### Supported SQL Features

The connector translates SQL WHERE clauses to LDAP filter syntax:

| SQL | LDAP Filter | Example |
|-----|-------------|---------|
| `=` | `(attr=value)` | `cn = 'John'` |
| `LIKE` with `%` | `(attr=value*)` | `cn LIKE 'John%'` |
| `AND` | `(&(...)(...))`| `cn = 'John' AND mail IS NOT NULL` |
| `OR` | `(\|(...)(...))`| `cn = 'John' OR cn = 'Jane'` |
| `NOT` | `(!(...))` | `NOT cn = 'Admin'` |
| `IS NULL` | `(!(attr=*))` | `mail IS NULL` |
| `IS NOT NULL` | `(attr=*)` | `mail IS NOT NULL` |
| `>=`, `<=` | `(attr>=value)` | `whenCreated >= '2024'` |
| `<>`, `!=` | `(!(attr=value))` | `cn <> 'Guest'` |

### Limitations

Operations handled by Dremio (not pushed to LDAP):

- **JOINs** - Dremio performs joins locally
- **Aggregations** - COUNT, SUM, GROUP BY computed by Dremio
- **ORDER BY** - Sorting done by Dremio
- **Subqueries** - Processed by Dremio

These don't affect functionality - Dremio handles them automatically.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Dremio    │────▶│  LDAP Connector  │────▶│  LDAP Server /  │
│   (SQL)     │     │  (JNDI-based)    │     │  Active Directory│
└─────────────┘     └──────────────────┘     └─────────────────┘
```

The connector:
1. Receives SQL queries from Dremio
2. Parses SELECT/FROM/WHERE clauses
3. Converts WHERE to LDAP filter syntax
4. Executes LDAP search via JNDI
5. Returns results as JDBC ResultSet

## Troubleshooting

### Enable Debug Logging

Add to `logback.xml`:
```xml
<logger name="com.dremio.exec.store.jdbc.conf" level="DEBUG"/>
```

### Common Issues

| Issue | Solution |
|-------|----------|
| "Operations error" | Use port 3268 (Global Catalog) for AD |
| 0 results for groups | Use `group` not `groupOfNames` in Object Classes |
| Authentication failed | Check Bind DN format and password |
| Size limit exceeded | Reduce Max Rows setting or add WHERE filters |

## ARP Framework

This connector uses Dremio's Advanced Relational Pushdown (ARP) Framework. Configuration in `src/main/resources/arp/implementation/ldap-arp.yaml` defines:

- Data type mappings (all LDAP attributes → VARCHAR)
- Supported SQL operations for pushdown
- Query syntax customizations

## License

Apache License 2.0

## Resources

- [Dremio Documentation](https://docs.dremio.com/)
- [JNDI LDAP Tutorial](https://docs.oracle.com/javase/tutorial/jndi/ldap/)
- [Active Directory Schema](https://docs.microsoft.com/en-us/windows/win32/adschema/active-directory-schema)
