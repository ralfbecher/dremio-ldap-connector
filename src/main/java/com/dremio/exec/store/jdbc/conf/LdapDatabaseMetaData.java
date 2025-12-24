package com.dremio.exec.store.jdbc.conf;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A DatabaseMetaData wrapper that handles the LDAP JDBC driver's limited
 * metadata support.
 *
 * The LDAP JDBC driver returns null for several metadata methods that
 * Dremio expects to return valid ResultSets. This wrapper intercepts
 * those calls and returns appropriate non-null values.
 *
 * Additionally, this wrapper exposes configured LDAP object classes as tables,
 * since the LDAP driver doesn't implement table discovery.
 */
public class LdapDatabaseMetaData implements DatabaseMetaData {
    private static final Logger LOG = Logger.getLogger(LdapDatabaseMetaData.class.getName());
    private final DatabaseMetaData delegate;
    private final Connection connection;
    private final String[] objectClasses;
    private final String[] attributes;

    public LdapDatabaseMetaData(DatabaseMetaData delegate, Connection connection, String[] objectClasses, String[] attributes) {
        this.delegate = delegate;
        this.connection = connection;
        this.objectClasses = objectClasses != null ? objectClasses : new String[0];
        this.attributes = attributes != null ? attributes : new String[0];
        LOG.log(Level.INFO, "LdapDatabaseMetaData created with " + this.objectClasses.length + " object classes and " + this.attributes.length + " attributes");
        if (this.objectClasses.length > 0) {
            LOG.log(Level.INFO, "Object classes: " + String.join(", ", this.objectClasses));
        }
        if (this.attributes.length > 0) {
            LOG.log(Level.INFO, "Attributes: " + String.join(", ", this.attributes));
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        // Return TABLE as the only supported type
        return new SingleValueResultSet("TABLE_TYPE", "TABLE");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        LOG.log(Level.INFO, "getTables called: catalog=" + catalog + ", schema=" + schemaPattern + ", tablePattern=" + tableNamePattern);
        // Return configured object classes as tables
        if (objectClasses.length > 0) {
            LOG.log(Level.INFO, "Returning ObjectClassResultSet with " + objectClasses.length + " object classes");
            return new ObjectClassResultSet(objectClasses, tableNamePattern);
        }
        // Fall back to delegate if no object classes configured
        LOG.log(Level.WARNING, "No object classes configured, falling back to delegate");
        ResultSet rs = delegate.getTables(catalog, schemaPattern, tableNamePattern, types);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"});
        }
        return rs;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        ResultSet rs = delegate.getSchemas();
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_SCHEM", "TABLE_CATALOG"});
        }
        return rs;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        ResultSet rs = delegate.getSchemas(catalog, schemaPattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_SCHEM", "TABLE_CATALOG"});
        }
        return rs;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        ResultSet rs = delegate.getCatalogs();
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT"});
        }
        return rs;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        LOG.log(Level.INFO, "getColumns called: catalog=" + catalog + ", schema=" + schemaPattern +
            ", tablePattern=" + tableNamePattern + ", columnPattern=" + columnNamePattern);
        // Return configured attributes as columns for all tables (object classes)
        if (attributes.length > 0 && objectClasses.length > 0) {
            LOG.log(Level.INFO, "Returning AttributeResultSet with " + attributes.length + " attributes for " + objectClasses.length + " tables");
            AttributeResultSet rs = new AttributeResultSet(objectClasses, attributes, tableNamePattern, columnNamePattern);
            LOG.log(Level.INFO, "AttributeResultSet created, checking row count...");
            return rs;
        }
        // Fall back to delegate if no attributes configured
        LOG.log(Level.WARNING, "No attributes or object classes configured (attrs=" + attributes.length + ", ocs=" + objectClasses.length + "), falling back to delegate");
        ResultSet rs = delegate.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME"});
        }
        return rs;
    }

    // Delegate all other methods

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return delegate.allProceduresAreCallable();
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return delegate.allTablesAreSelectable();
    }

    @Override
    public String getURL() throws SQLException {
        return delegate.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return delegate.getUserName();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return delegate.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return delegate.nullsAreSortedHigh();
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return delegate.nullsAreSortedLow();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return delegate.nullsAreSortedAtStart();
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return delegate.nullsAreSortedAtEnd();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return delegate.getDatabaseProductName();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return delegate.getDatabaseProductVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return delegate.getDriverName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return delegate.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return delegate.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return delegate.getDriverMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return delegate.usesLocalFiles();
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return delegate.usesLocalFilePerTable();
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return delegate.supportsMixedCaseIdentifiers();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return delegate.storesUpperCaseIdentifiers();
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return delegate.storesLowerCaseIdentifiers();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return delegate.storesMixedCaseIdentifiers();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return delegate.supportsMixedCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return delegate.storesUpperCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return delegate.storesLowerCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return delegate.storesMixedCaseQuotedIdentifiers();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return delegate.getIdentifierQuoteString();
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return delegate.getSQLKeywords();
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return delegate.getNumericFunctions();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return delegate.getStringFunctions();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return delegate.getSystemFunctions();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return delegate.getTimeDateFunctions();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return delegate.getSearchStringEscape();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return delegate.getExtraNameCharacters();
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return delegate.supportsAlterTableWithAddColumn();
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return delegate.supportsAlterTableWithDropColumn();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return delegate.supportsColumnAliasing();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return delegate.nullPlusNonNullIsNull();
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return delegate.supportsConvert();
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return delegate.supportsConvert(fromType, toType);
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return delegate.supportsTableCorrelationNames();
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return delegate.supportsDifferentTableCorrelationNames();
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return delegate.supportsExpressionsInOrderBy();
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return delegate.supportsOrderByUnrelated();
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return delegate.supportsGroupBy();
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return delegate.supportsGroupByUnrelated();
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return delegate.supportsGroupByBeyondSelect();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return delegate.supportsLikeEscapeClause();
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return delegate.supportsMultipleResultSets();
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return delegate.supportsMultipleTransactions();
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return delegate.supportsNonNullableColumns();
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return delegate.supportsMinimumSQLGrammar();
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return delegate.supportsCoreSQLGrammar();
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return delegate.supportsExtendedSQLGrammar();
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return delegate.supportsANSI92EntryLevelSQL();
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return delegate.supportsANSI92IntermediateSQL();
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return delegate.supportsANSI92FullSQL();
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return delegate.supportsIntegrityEnhancementFacility();
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return delegate.supportsOuterJoins();
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return delegate.supportsFullOuterJoins();
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return delegate.supportsLimitedOuterJoins();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return delegate.getSchemaTerm();
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return delegate.getProcedureTerm();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return delegate.getCatalogTerm();
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return delegate.isCatalogAtStart();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return delegate.getCatalogSeparator();
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return delegate.supportsSchemasInDataManipulation();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return delegate.supportsSchemasInProcedureCalls();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return delegate.supportsSchemasInTableDefinitions();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return delegate.supportsSchemasInIndexDefinitions();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return delegate.supportsSchemasInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return delegate.supportsCatalogsInDataManipulation();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return delegate.supportsCatalogsInProcedureCalls();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return delegate.supportsCatalogsInTableDefinitions();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return delegate.supportsCatalogsInIndexDefinitions();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return delegate.supportsCatalogsInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return delegate.supportsPositionedDelete();
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return delegate.supportsPositionedUpdate();
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return delegate.supportsSelectForUpdate();
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return delegate.supportsStoredProcedures();
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return delegate.supportsSubqueriesInComparisons();
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return delegate.supportsSubqueriesInExists();
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return delegate.supportsSubqueriesInIns();
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return delegate.supportsSubqueriesInQuantifieds();
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return delegate.supportsCorrelatedSubqueries();
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return delegate.supportsUnion();
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return delegate.supportsUnionAll();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return delegate.supportsOpenCursorsAcrossCommit();
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return delegate.supportsOpenCursorsAcrossRollback();
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return delegate.supportsOpenStatementsAcrossCommit();
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return delegate.supportsOpenStatementsAcrossRollback();
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return delegate.getMaxBinaryLiteralLength();
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return delegate.getMaxCharLiteralLength();
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return delegate.getMaxColumnNameLength();
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return delegate.getMaxColumnsInGroupBy();
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return delegate.getMaxColumnsInIndex();
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return delegate.getMaxColumnsInOrderBy();
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return delegate.getMaxColumnsInSelect();
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return delegate.getMaxColumnsInTable();
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return delegate.getMaxConnections();
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return delegate.getMaxCursorNameLength();
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return delegate.getMaxIndexLength();
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return delegate.getMaxSchemaNameLength();
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return delegate.getMaxProcedureNameLength();
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return delegate.getMaxCatalogNameLength();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return delegate.getMaxRowSize();
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return delegate.doesMaxRowSizeIncludeBlobs();
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return delegate.getMaxStatementLength();
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return delegate.getMaxStatements();
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return delegate.getMaxTableNameLength();
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return delegate.getMaxTablesInSelect();
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return delegate.getMaxUserNameLength();
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
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
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        ResultSet rs = delegate.getProcedures(catalog, schemaPattern, procedureNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        ResultSet rs = delegate.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        ResultSet rs = delegate.getColumnPrivileges(catalog, schema, table, columnNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        ResultSet rs = delegate.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        ResultSet rs = delegate.getBestRowIdentifier(catalog, schema, table, scope, nullable);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE"});
        }
        return rs;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        ResultSet rs = delegate.getVersionColumns(catalog, schema, table);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE"});
        }
        return rs;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet rs = delegate.getPrimaryKeys(catalog, schema, table);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet rs = delegate.getImportedKeys(catalog, schema, table);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet rs = delegate.getExportedKeys(catalog, schema, table);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        ResultSet rs = delegate.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        ResultSet rs = delegate.getTypeInfo();
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TYPE_NAME", "DATA_TYPE", "PRECISION"});
        }
        return rs;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        ResultSet rs = delegate.getIndexInfo(catalog, schema, table, unique, approximate);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"});
        }
        return rs;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return delegate.supportsResultSetType(type);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return delegate.supportsResultSetConcurrency(type, concurrency);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return delegate.ownUpdatesAreVisible(type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return delegate.ownDeletesAreVisible(type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return delegate.ownInsertsAreVisible(type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return delegate.othersUpdatesAreVisible(type);
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return delegate.othersDeletesAreVisible(type);
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return delegate.othersInsertsAreVisible(type);
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return delegate.updatesAreDetected(type);
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return delegate.deletesAreDetected(type);
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return delegate.insertsAreDetected(type);
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return delegate.supportsBatchUpdates();
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        ResultSet rs = delegate.getUDTs(catalog, schemaPattern, typeNamePattern, types);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME"});
        }
        return rs;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return delegate.supportsNamedParameters();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return delegate.supportsMultipleOpenResults();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return delegate.supportsGetGeneratedKeys();
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        ResultSet rs = delegate.getSuperTypes(catalog, schemaPattern, typeNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        ResultSet rs = delegate.getSuperTables(catalog, schemaPattern, tableNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        ResultSet rs = delegate.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME"});
        }
        return rs;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return delegate.supportsResultSetHoldability(holdability);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return delegate.getDatabaseMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return delegate.getDatabaseMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return delegate.getJDBCMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return delegate.getJDBCMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return delegate.getSQLStateType();
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return delegate.locatorsUpdateCopy();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return delegate.supportsStatementPooling();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return delegate.getRowIdLifetime();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return delegate.supportsStoredFunctionsUsingCallSyntax();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return delegate.autoCommitFailureClosesAllResultSets();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        ResultSet rs = delegate.getClientInfoProperties();
        if (rs == null) {
            return new EmptyResultSet(new String[]{"NAME", "MAX_LEN", "DEFAULT_VALUE"});
        }
        return rs;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        ResultSet rs = delegate.getFunctions(catalog, schemaPattern, functionNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        ResultSet rs = delegate.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME"});
        }
        return rs;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        ResultSet rs = delegate.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        if (rs == null) {
            return new EmptyResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"});
        }
        return rs;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return delegate.generatedKeyAlwaysReturned();
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
