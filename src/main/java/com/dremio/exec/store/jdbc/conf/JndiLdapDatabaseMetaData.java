package com.dremio.exec.store.jdbc.conf;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DatabaseMetaData implementation for JNDI LDAP connection.
 * Exposes configured objectClasses as tables and attributes as columns.
 */
public class JndiLdapDatabaseMetaData implements DatabaseMetaData {
    private static final Logger LOG = Logger.getLogger(JndiLdapDatabaseMetaData.class.getName());

    private final JndiLdapConnection connection;

    public JndiLdapDatabaseMetaData(JndiLdapConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        LOG.log(Level.INFO, "getTables called: catalog=" + catalog + ", schema=" + schemaPattern + ", table=" + tableNamePattern);

        // Return configured objectClasses as tables
        String[] objectClasses = connection.getObjectClasses();
        List<Map<String, Object>> tables = new ArrayList<>();

        for (String oc : objectClasses) {
            if (tableNamePattern == null || "%".equals(tableNamePattern) || oc.equalsIgnoreCase(tableNamePattern)) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("TABLE_CAT", null);
                row.put("TABLE_SCHEM", null);
                row.put("TABLE_NAME", oc);
                row.put("TABLE_TYPE", "TABLE");
                row.put("REMARKS", "LDAP objectClass: " + oc);
                row.put("TYPE_CAT", null);
                row.put("TYPE_SCHEM", null);
                row.put("TYPE_NAME", null);
                row.put("SELF_REFERENCING_COL_NAME", null);
                row.put("REF_GENERATION", null);
                tables.add(row);
            }
        }

        LOG.log(Level.INFO, "Returning " + tables.size() + " tables");
        String[] columns = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"};
        return new JndiLdapResultSet(tables, columns);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        LOG.log(Level.INFO, "getColumns called: table=" + tableNamePattern + ", column=" + columnNamePattern);

        // Return configured attributes as columns for all matching tables
        String[] objectClasses = connection.getObjectClasses();
        String[] attributes = connection.getAttributes();
        List<Map<String, Object>> columns = new ArrayList<>();

        for (String oc : objectClasses) {
            if (tableNamePattern == null || "%".equals(tableNamePattern) || oc.equalsIgnoreCase(tableNamePattern)) {
                int ordinal = 1;
                for (String attr : attributes) {
                    if (columnNamePattern == null || "%".equals(columnNamePattern) || attr.equalsIgnoreCase(columnNamePattern)) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("TABLE_CAT", null);
                        row.put("TABLE_SCHEM", null);
                        row.put("TABLE_NAME", oc);
                        row.put("COLUMN_NAME", attr);
                        row.put("DATA_TYPE", Types.VARCHAR);
                        row.put("TYPE_NAME", "VARCHAR");
                        row.put("COLUMN_SIZE", 4000);
                        row.put("BUFFER_LENGTH", 0);
                        row.put("DECIMAL_DIGITS", 0);
                        row.put("NUM_PREC_RADIX", 10);
                        row.put("NULLABLE", DatabaseMetaData.columnNullable);
                        row.put("REMARKS", "LDAP attribute");
                        row.put("COLUMN_DEF", null);
                        row.put("SQL_DATA_TYPE", 0);
                        row.put("SQL_DATETIME_SUB", 0);
                        row.put("CHAR_OCTET_LENGTH", 4000);
                        row.put("ORDINAL_POSITION", ordinal++);
                        row.put("IS_NULLABLE", "YES");
                        row.put("SCOPE_CATALOG", null);
                        row.put("SCOPE_SCHEMA", null);
                        row.put("SCOPE_TABLE", null);
                        row.put("SOURCE_DATA_TYPE", null);
                        row.put("IS_AUTOINCREMENT", "NO");
                        row.put("IS_GENERATEDCOLUMN", "NO");
                        columns.add(row);
                    }
                }
            }
        }

        LOG.log(Level.INFO, "Returning " + columns.size() + " columns");
        String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
                "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"};
        return new JndiLdapResultSet(columns, colNames);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        // Return empty schema list - LDAP doesn't have schemas
        return new JndiLdapResultSet(new ArrayList<>(), new String[]{"TABLE_SCHEM", "TABLE_CATALOG"});
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        // Return empty catalog list
        return new JndiLdapResultSet(new ArrayList<>(), new String[]{"TABLE_CAT"});
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<Map<String, Object>> types = new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_TYPE", "TABLE");
        types.add(row);
        return new JndiLdapResultSet(types, new String[]{"TABLE_TYPE"});
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // DN is the primary key for LDAP entries
        List<Map<String, Object>> keys = new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_CAT", null);
        row.put("TABLE_SCHEM", null);
        row.put("TABLE_NAME", table);
        row.put("COLUMN_NAME", "dn");
        row.put("KEY_SEQ", 1);
        row.put("PK_NAME", "PK_DN");
        keys.add(row);
        return new JndiLdapResultSet(keys, new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"});
    }

    // Connection info
    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public String getURL() throws SQLException {
        return "jdbc:ldap:" + connection.getLdapUrl() + "/" + connection.getBaseDN();
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getPrincipal();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "LDAP";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Dremio JNDI LDAP Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    // Feature support
    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    // Identifier handling
    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    // Limits
    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0; // No limit
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 128;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 128;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    // Empty result sets for unsupported features
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<Map<String, Object>> types = new ArrayList<>();
        Map<String, Object> varchar = new LinkedHashMap<>();
        varchar.put("TYPE_NAME", "VARCHAR");
        varchar.put("DATA_TYPE", Types.VARCHAR);
        varchar.put("PRECISION", 4000);
        varchar.put("LITERAL_PREFIX", "'");
        varchar.put("LITERAL_SUFFIX", "'");
        varchar.put("CREATE_PARAMS", null);
        varchar.put("NULLABLE", DatabaseMetaData.typeNullable);
        varchar.put("CASE_SENSITIVE", false);
        varchar.put("SEARCHABLE", DatabaseMetaData.typeSearchable);
        varchar.put("UNSIGNED_ATTRIBUTE", false);
        varchar.put("FIXED_PREC_SCALE", false);
        varchar.put("AUTO_INCREMENT", false);
        varchar.put("LOCAL_TYPE_NAME", "VARCHAR");
        varchar.put("MINIMUM_SCALE", 0);
        varchar.put("MAXIMUM_SCALE", 0);
        varchar.put("SQL_DATA_TYPE", 0);
        varchar.put("SQL_DATETIME_SUB", 0);
        varchar.put("NUM_PREC_RADIX", 10);
        types.add(varchar);

        String[] cols = {"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
                "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};
        return new JndiLdapResultSet(types, cols);
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    private ResultSet emptyResultSet() {
        return new JndiLdapResultSet(new ArrayList<>(), new String[0]);
    }

    // Remaining methods with simple implementations
    @Override public boolean allProceduresAreCallable() throws SQLException { return false; }
    @Override public boolean allTablesAreSelectable() throws SQLException { return true; }
    @Override public boolean nullsAreSortedHigh() throws SQLException { return false; }
    @Override public boolean nullsAreSortedLow() throws SQLException { return true; }
    @Override public boolean nullsAreSortedAtStart() throws SQLException { return false; }
    @Override public boolean nullsAreSortedAtEnd() throws SQLException { return false; }
    @Override public boolean usesLocalFiles() throws SQLException { return false; }
    @Override public boolean usesLocalFilePerTable() throws SQLException { return false; }
    @Override public String getNumericFunctions() throws SQLException { return ""; }
    @Override public String getStringFunctions() throws SQLException { return ""; }
    @Override public String getSystemFunctions() throws SQLException { return ""; }
    @Override public String getTimeDateFunctions() throws SQLException { return ""; }
    @Override public String getSearchStringEscape() throws SQLException { return "\\"; }
    @Override public String getProcedureTerm() throws SQLException { return "procedure"; }
    @Override public String getCatalogTerm() throws SQLException { return "catalog"; }
    @Override public String getSchemaTerm() throws SQLException { return "schema"; }
    @Override public boolean isCatalogAtStart() throws SQLException { return true; }
    @Override public String getCatalogSeparator() throws SQLException { return "."; }
    @Override public boolean supportsSchemasInDataManipulation() throws SQLException { return false; }
    @Override public boolean supportsSchemasInProcedureCalls() throws SQLException { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() throws SQLException { return false; }
    @Override public boolean supportsSchemasInIndexDefinitions() throws SQLException { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() throws SQLException { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException { return false; }
    @Override public boolean supportsPositionedDelete() throws SQLException { return false; }
    @Override public boolean supportsPositionedUpdate() throws SQLException { return false; }
    @Override public boolean supportsAlterTableWithAddColumn() throws SQLException { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() throws SQLException { return false; }
    @Override public boolean supportsColumnAliasing() throws SQLException { return true; }
    @Override public boolean nullPlusNonNullIsNull() throws SQLException { return true; }
    @Override public boolean supportsConvert() throws SQLException { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) throws SQLException { return false; }
    @Override public boolean supportsTableCorrelationNames() throws SQLException { return false; }
    @Override public boolean supportsDifferentTableCorrelationNames() throws SQLException { return false; }
    @Override public boolean supportsExpressionsInOrderBy() throws SQLException { return false; }
    @Override public boolean supportsGroupByUnrelated() throws SQLException { return false; }
    @Override public boolean supportsGroupByBeyondSelect() throws SQLException { return false; }
    @Override public boolean supportsLikeEscapeClause() throws SQLException { return false; }
    @Override public boolean supportsMultipleResultSets() throws SQLException { return false; }
    @Override public boolean supportsMultipleTransactions() throws SQLException { return false; }
    @Override public boolean supportsNonNullableColumns() throws SQLException { return false; }
    @Override public boolean supportsMinimumSQLGrammar() throws SQLException { return true; }
    @Override public boolean supportsCoreSQLGrammar() throws SQLException { return false; }
    @Override public boolean supportsExtendedSQLGrammar() throws SQLException { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() throws SQLException { return false; }
    @Override public boolean supportsANSI92IntermediateSQL() throws SQLException { return false; }
    @Override public boolean supportsANSI92FullSQL() throws SQLException { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() throws SQLException { return false; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() throws SQLException { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() throws SQLException { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() throws SQLException { return false; }
    @Override public boolean supportsResultSetType(int type) throws SQLException { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException { return concurrency == ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean ownUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean ownDeletesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean ownInsertsAreVisible(int type) throws SQLException { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean othersDeletesAreVisible(int type) throws SQLException { return false; }
    @Override public boolean othersInsertsAreVisible(int type) throws SQLException { return false; }
    @Override public boolean updatesAreDetected(int type) throws SQLException { return false; }
    @Override public boolean deletesAreDetected(int type) throws SQLException { return false; }
    @Override public boolean insertsAreDetected(int type) throws SQLException { return false; }
    @Override public boolean supportsNamedParameters() throws SQLException { return false; }
    @Override public boolean supportsMultipleOpenResults() throws SQLException { return false; }
    @Override public boolean supportsGetGeneratedKeys() throws SQLException { return false; }
    @Override public boolean supportsResultSetHoldability(int holdability) throws SQLException { return false; }
    @Override public int getResultSetHoldability() throws SQLException { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public int getSQLStateType() throws SQLException { return DatabaseMetaData.sqlStateSQL; }
    @Override public boolean locatorsUpdateCopy() throws SQLException { return false; }
    @Override public boolean supportsStatementPooling() throws SQLException { return false; }
    @Override public RowIdLifetime getRowIdLifetime() throws SQLException { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() throws SQLException { return false; }
    @Override public boolean generatedKeyAlwaysReturned() throws SQLException { return false; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() throws SQLException { return false; }
    @Override public int getDefaultTransactionIsolation() throws SQLException { return Connection.TRANSACTION_NONE; }
    @Override public boolean supportsOpenStatementsAcrossCommit() throws SQLException { return false; }
    @Override public boolean supportsOpenStatementsAcrossRollback() throws SQLException { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() throws SQLException { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() throws SQLException { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
