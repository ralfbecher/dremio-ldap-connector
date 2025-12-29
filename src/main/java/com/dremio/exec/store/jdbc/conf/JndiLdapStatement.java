package com.dremio.exec.store.jdbc.conf;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;

/**
 * A Statement implementation that executes LDAP queries directly using JNDI.
 *
 * This bypasses the broken Octetstring JDBC-LDAP driver and uses Java's built-in
 * JNDI LDAP support to execute queries against Active Directory.
 */
public class JndiLdapStatement implements Statement {
    private static final Logger LOG = Logger.getLogger(JndiLdapStatement.class.getName());

    // Pattern to parse SQL: SELECT columns FROM baseDN [WHERE filter]
    private static final Pattern SELECT_PATTERN = Pattern.compile(
        "^\\s*SELECT\\s+(.+?)\\s+FROM\\s+(.+?)(?:\\s+WHERE\\s+(.+))?\\s*$",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private final String ldapUrl;
    private final String baseDN;
    private final String principal;
    private final String credentials;
    private final String[] objectClasses;
    private final String[] configuredAttributes;
    private final int maxRows;
    private final boolean useObjectCategory;
    private final boolean skipFilter;

    private int queryMaxRows = 0;
    private int queryTimeout = 0;
    private boolean closed = false;
    private ResultSet currentResultSet = null;
    private Connection connection;

    public JndiLdapStatement(Connection connection, String ldapUrl, String baseDN,
            String principal, String credentials,
            String[] objectClasses, String[] configuredAttributes,
            int maxRows, boolean useObjectCategory, boolean skipFilter) {
        this.connection = connection;
        this.ldapUrl = ldapUrl;
        this.baseDN = baseDN;
        this.principal = principal;
        this.credentials = credentials;
        this.objectClasses = objectClasses != null ? objectClasses : new String[0];
        this.configuredAttributes = configuredAttributes != null ? configuredAttributes : new String[0];
        this.maxRows = maxRows;
        this.useObjectCategory = useObjectCategory;
        this.skipFilter = skipFilter;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        LOG.log(Level.WARNING, "=== JndiLdapStatement.executeQuery ===");
        LOG.log(Level.WARNING, "SQL: " + sql);

        try {
            // Parse the SQL
            Matcher matcher = SELECT_PATTERN.matcher(sql.trim());
            if (!matcher.matches()) {
                throw new SQLException("Cannot parse SQL: " + sql);
            }

            String selectPart = matcher.group(1).trim();
            String fromPart = matcher.group(2).trim();
            String wherePart = matcher.group(3);

            LOG.log(Level.WARNING, "SELECT: " + selectPart);
            LOG.log(Level.WARNING, "FROM: " + fromPart);
            LOG.log(Level.WARNING, "WHERE: " + wherePart);

            // Determine requested attributes
            String[] requestedAttrs;
            if ("*".equals(selectPart.trim())) {
                requestedAttrs = configuredAttributes.length > 0 ? configuredAttributes : new String[]{"dn", "cn", "objectClass"};
                LOG.log(Level.WARNING, "SELECT * - using configured attributes");
            } else {
                requestedAttrs = selectPart.split(",");
                for (int i = 0; i < requestedAttrs.length; i++) {
                    String attr = requestedAttrs[i].trim();
                    // Remove quotes
                    attr = attr.replace("\"", "");
                    // Remove table prefix if present (e.g., person.givenName -> givenName)
                    int dotIdx = attr.lastIndexOf('.');
                    if (dotIdx >= 0) {
                        attr = attr.substring(dotIdx + 1);
                    }
                    requestedAttrs[i] = attr;
                    LOG.log(Level.WARNING, "Parsed column " + i + ": '" + requestedAttrs[i] + "' from '" + selectPart.split(",")[i].trim() + "'");
                }
            }
            LOG.log(Level.WARNING, "Final requested attributes: " + String.join(", ", requestedAttrs));

            // Determine search base - check if FROM is an objectClass name or a DN
            String searchBase = baseDN;
            String objectClassFilter = null;

            // Check if fromPart is one of our configured objectClasses
            String fromPartClean = fromPart.replace("\"", "").trim();
            for (String oc : objectClasses) {
                if (oc.equalsIgnoreCase(fromPartClean)) {
                    objectClassFilter = oc;
                    LOG.log(Level.WARNING, "FROM is objectClass: " + oc + ", using baseDN: " + baseDN);
                    break;
                }
            }

            if (objectClassFilter == null) {
                // FROM is probably a DN itself
                searchBase = fromPartClean;
                LOG.log(Level.WARNING, "FROM is DN: " + searchBase);
            }

            // Build LDAP filter
            String ldapFilter = buildLdapFilter(wherePart, objectClassFilter);
            LOG.log(Level.WARNING, "LDAP filter: " + ldapFilter);

            // Execute LDAP search
            int effectiveMaxRows = queryMaxRows > 0 ? queryMaxRows : maxRows;
            List<Map<String, Object>> results = executeLdapSearch(searchBase, ldapFilter, requestedAttrs, effectiveMaxRows);

            LOG.log(Level.WARNING, "Search returned " + results.size() + " results");

            // Create ResultSet
            currentResultSet = new JndiLdapResultSet(results, requestedAttrs);
            return currentResultSet;

        } catch (NamingException e) {
            LOG.log(Level.SEVERE, "=== LDAP SEARCH FAILED ===");
            LOG.log(Level.SEVERE, "Exception type: " + e.getClass().getName());
            LOG.log(Level.SEVERE, "Message: " + e.getMessage());
            LOG.log(Level.SEVERE, "Explanation: " + e.getExplanation());
            if (e.getRemainingName() != null) {
                LOG.log(Level.SEVERE, "Remaining name: " + e.getRemainingName());
            }
            if (e.getResolvedName() != null) {
                LOG.log(Level.SEVERE, "Resolved name: " + e.getResolvedName());
            }
            // Check for specific LDAP exceptions
            if (e instanceof javax.naming.ldap.LdapReferralException) {
                LOG.log(Level.SEVERE, "Error type: LDAP Referral");
            } else if (e instanceof javax.naming.AuthenticationException) {
                LOG.log(Level.SEVERE, "Error type: Authentication failure");
            } else if (e instanceof javax.naming.CommunicationException) {
                LOG.log(Level.SEVERE, "Error type: Communication error");
            } else if (e instanceof javax.naming.InvalidNameException) {
                LOG.log(Level.SEVERE, "Error type: Invalid DN or filter syntax");
            } else if (e instanceof javax.naming.OperationNotSupportedException) {
                LOG.log(Level.SEVERE, "Error type: Operation not supported");
            }
            Throwable cause = e.getCause();
            while (cause != null) {
                LOG.log(Level.SEVERE, "  Caused by: " + cause.getClass().getName() + ": " + cause.getMessage());
                cause = cause.getCause();
            }
            e.printStackTrace();
            throw new SQLException("LDAP search failed: " + e.getMessage(), e);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "=== UNEXPECTED ERROR IN LDAP QUERY ===");
            LOG.log(Level.SEVERE, "Exception type: " + e.getClass().getName());
            LOG.log(Level.SEVERE, "Message: " + e.getMessage());
            e.printStackTrace();
            throw new SQLException("LDAP query error: " + e.getMessage(), e);
        }
    }

    /**
     * Build an LDAP filter from SQL WHERE clause.
     */
    private String buildLdapFilter(String wherePart, String objectClassFilter) {
        LOG.log(Level.WARNING, "buildLdapFilter: objectClassFilter=" + objectClassFilter + ", skipFilter=" + skipFilter);
        LOG.log(Level.WARNING, "buildLdapFilter: wherePart=" + wherePart);
        LOG.log(Level.WARNING, "buildLdapFilter: useObjectCategory=" + useObjectCategory);

        StringBuilder filter = new StringBuilder();

        // Start with AND group if we have multiple conditions
        boolean hasObjectClassFilter = objectClassFilter != null && !skipFilter;
        boolean hasWhereClause = wherePart != null && !wherePart.trim().isEmpty();
        LOG.log(Level.WARNING, "buildLdapFilter: hasObjectClassFilter=" + hasObjectClassFilter + ", hasWhereClause=" + hasWhereClause);

        if (hasObjectClassFilter && hasWhereClause) {
            filter.append("(&");
        }

        // Add objectClass/objectCategory filter
        if (hasObjectClassFilter) {
            String filterAttr = useObjectCategory ? "objectCategory" : "objectClass";
            filter.append("(").append(filterAttr).append("=").append(objectClassFilter).append(")");
        }

        // Convert WHERE clause to LDAP filter
        if (hasWhereClause) {
            String ldapWhere = convertWhereToLdap(wherePart);
            filter.append(ldapWhere);
        }

        if (hasObjectClassFilter && hasWhereClause) {
            filter.append(")");
        }

        // Default filter if nothing specified
        if (filter.length() == 0) {
            filter.append("(objectClass=*)");
        }

        String finalFilter = filter.toString();
        LOG.log(Level.WARNING, "buildLdapFilter: FINAL FILTER = " + finalFilter);
        return finalFilter;
    }

    /**
     * Convert SQL WHERE clause to LDAP filter syntax.
     * Supports: AND, OR, NOT, LIKE, =, >=, <=
     */
    private String convertWhereToLdap(String wherePart) {
        if (wherePart == null || wherePart.trim().isEmpty()) return "";

        String w = wherePart.trim();

        // If it already looks like LDAP filter, use it
        if (w.startsWith("(") && w.endsWith(")") && !w.toUpperCase().contains(" AND ")
                && !w.toUpperCase().contains(" OR ")) {
            return w;
        }

        // Parse and convert the WHERE clause
        return parseWhereClause(w);
    }

    /**
     * Parse a SQL WHERE clause and convert to LDAP filter.
     */
    private String parseWhereClause(String where) {
        where = where.trim();

        // Handle parentheses groups
        if (where.startsWith("(") && where.endsWith(")")) {
            // Check if these are matching outer parentheses
            int depth = 0;
            boolean isOuter = true;
            for (int i = 0; i < where.length() - 1; i++) {
                if (where.charAt(i) == '(') depth++;
                else if (where.charAt(i) == ')') depth--;
                if (depth == 0 && i < where.length() - 1) {
                    isOuter = false;
                    break;
                }
            }
            if (isOuter) {
                return parseWhereClause(where.substring(1, where.length() - 1));
            }
        }

        // Handle NOT (must check before AND/OR)
        if (where.toUpperCase().startsWith("NOT ")) {
            String inner = where.substring(4).trim();
            return "(!" + parseWhereClause(inner) + ")";
        }

        // Handle AND (split on AND outside of parentheses)
        int andPos = findOperatorPosition(where, " AND ");
        if (andPos > 0) {
            String left = where.substring(0, andPos).trim();
            String right = where.substring(andPos + 5).trim();
            return "(&" + parseWhereClause(left) + parseWhereClause(right) + ")";
        }

        // Handle OR (split on OR outside of parentheses)
        int orPos = findOperatorPosition(where, " OR ");
        if (orPos > 0) {
            String left = where.substring(0, orPos).trim();
            String right = where.substring(orPos + 4).trim();
            return "(|" + parseWhereClause(left) + parseWhereClause(right) + ")";
        }

        // Handle individual conditions
        return parseCondition(where);
    }

    /**
     * Find the position of an operator outside of parentheses.
     */
    private int findOperatorPosition(String str, String operator) {
        String upperStr = str.toUpperCase();
        String upperOp = operator.toUpperCase();
        int depth = 0;
        int pos = 0;

        while (pos < str.length()) {
            if (str.charAt(pos) == '(') depth++;
            else if (str.charAt(pos) == ')') depth--;

            if (depth == 0 && upperStr.substring(pos).startsWith(upperOp)) {
                return pos;
            }
            pos++;
        }
        return -1;
    }

    /**
     * Parse a single condition (attr op value).
     */
    private String parseCondition(String condition) {
        condition = condition.trim();

        // Already LDAP format
        if (condition.startsWith("(") && condition.endsWith(")")) {
            return condition;
        }

        // Handle LIKE -> LDAP wildcard
        // SQL: cn LIKE 'John%' -> LDAP: (cn=John*)
        // SQL: cn LIKE '%John' -> LDAP: (cn=*John)
        // SQL: cn LIKE '%John%' -> LDAP: (cn=*John*)
        Pattern likePattern = Pattern.compile(
            "(\\w+)\\s+LIKE\\s+'([^']*)'",
            Pattern.CASE_INSENSITIVE
        );
        Matcher likeMatcher = likePattern.matcher(condition);
        if (likeMatcher.matches()) {
            String attr = likeMatcher.group(1);
            String value = likeMatcher.group(2);
            // Convert SQL % to LDAP *
            value = value.replace("%", "*");
            // Convert SQL _ to LDAP ? (single char wildcard) - note: not all LDAP servers support this
            value = value.replace("_", "?");
            return "(" + attr + "=" + value + ")";
        }

        // Handle NOT LIKE
        Pattern notLikePattern = Pattern.compile(
            "(\\w+)\\s+NOT\\s+LIKE\\s+'([^']*)'",
            Pattern.CASE_INSENSITIVE
        );
        Matcher notLikeMatcher = notLikePattern.matcher(condition);
        if (notLikeMatcher.matches()) {
            String attr = notLikeMatcher.group(1);
            String value = notLikeMatcher.group(2).replace("%", "*");
            return "(!(" + attr + "=" + value + "))";
        }

        // Handle IS NULL -> LDAP: (!(attr=*))
        Pattern isNullPattern = Pattern.compile(
            "(\\w+)\\s+IS\\s+NULL",
            Pattern.CASE_INSENSITIVE
        );
        Matcher isNullMatcher = isNullPattern.matcher(condition);
        if (isNullMatcher.matches()) {
            String attr = isNullMatcher.group(1);
            return "(!(" + attr + "=*))";
        }

        // Handle IS NOT NULL -> LDAP: (attr=*)
        Pattern isNotNullPattern = Pattern.compile(
            "(\\w+)\\s+IS\\s+NOT\\s+NULL",
            Pattern.CASE_INSENSITIVE
        );
        Matcher isNotNullMatcher = isNotNullPattern.matcher(condition);
        if (isNotNullMatcher.matches()) {
            String attr = isNotNullMatcher.group(1);
            return "(" + attr + "=*)";
        }

        // Handle >= (greater than or equal)
        Pattern gePattern = Pattern.compile("(\\w+)\\s*>=\\s*'?([^']*)'?");
        Matcher geMatcher = gePattern.matcher(condition);
        if (geMatcher.matches()) {
            return "(" + geMatcher.group(1) + ">=" + geMatcher.group(2).trim() + ")";
        }

        // Handle <= (less than or equal)
        Pattern lePattern = Pattern.compile("(\\w+)\\s*<=\\s*'?([^']*)'?");
        Matcher leMatcher = lePattern.matcher(condition);
        if (leMatcher.matches()) {
            return "(" + leMatcher.group(1) + "<=" + leMatcher.group(2).trim() + ")";
        }

        // Handle <> or != (not equal) -> LDAP: (!(attr=value))
        Pattern nePattern = Pattern.compile("(\\w+)\\s*(<>|!=)\\s*'?([^']*)'?");
        Matcher neMatcher = nePattern.matcher(condition);
        if (neMatcher.matches()) {
            return "(!(" + neMatcher.group(1) + "=" + neMatcher.group(3).trim() + "))";
        }

        // Handle simple equality: attr='value' or attr=value
        Pattern eqPattern = Pattern.compile("(\\w+)\\s*=\\s*'?([^']*)'?");
        Matcher eqMatcher = eqPattern.matcher(condition);
        if (eqMatcher.matches()) {
            String attr = eqMatcher.group(1);
            String value = eqMatcher.group(2).trim();
            // Remove trailing quote if present
            if (value.endsWith("'")) {
                value = value.substring(0, value.length() - 1);
            }
            return "(" + attr + "=" + value + ")";
        }

        // Fallback: wrap as-is
        LOG.log(Level.WARNING, "Could not parse condition, using as-is: " + condition);
        return "(" + condition + ")";
    }

    /**
     * Execute LDAP search using JNDI.
     */
    private List<Map<String, Object>> executeLdapSearch(String searchBase, String filter,
            String[] attributes, int maxResults) throws NamingException {

        List<Map<String, Object>> results = new ArrayList<>();

        // Set up JNDI environment
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");

        // Build the full provider URL
        String providerUrl = ldapUrl + "/" + searchBase;
        LOG.log(Level.WARNING, "JNDI Provider URL: " + providerUrl);
        LOG.log(Level.WARNING, "LDAP Filter: " + filter);
        LOG.log(Level.WARNING, "Requested attributes: " + String.join(", ", attributes));

        env.put(Context.PROVIDER_URL, providerUrl);

        if (principal != null && !principal.isEmpty()) {
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, principal);
            env.put(Context.SECURITY_CREDENTIALS, credentials != null ? credentials : "");
        }

        // Disable referral following to avoid AD referral errors
        env.put(Context.REFERRAL, "ignore");

        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(env);

            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            searchControls.setCountLimit(maxResults > 0 ? maxResults : 500);
            searchControls.setReturningAttributes(attributes);

            LOG.log(Level.WARNING, "Executing JNDI search: base=" + searchBase + ", filter=" + filter + ", maxResults=" + maxResults);

            NamingEnumeration<SearchResult> searchResults = ctx.search("", filter, searchControls);

            // Check if dn was explicitly requested
            Set<String> requestedSet = new HashSet<>();
            for (String attr : attributes) {
                requestedSet.add(attr.toLowerCase());
            }
            boolean dnRequested = requestedSet.contains("dn") || requestedSet.contains("distinguishedname");

            while (searchResults.hasMore()) {
                try {
                    SearchResult sr = searchResults.next();
                    Map<String, Object> row = new LinkedHashMap<>();

                    // Only add DN if it was requested
                    if (dnRequested) {
                        String dn = sr.getNameInNamespace();
                        row.put("dn", dn);
                    }

                    // Add other attributes
                    Attributes attrs = sr.getAttributes();
                    if (attrs != null) {
                        NamingEnumeration<? extends Attribute> attrEnum = attrs.getAll();
                        while (attrEnum.hasMore()) {
                            Attribute attr = attrEnum.next();
                            String attrName = attr.getID();

                            // Handle multi-valued attributes
                            if (attr.size() == 1) {
                                row.put(attrName, attr.get());
                            } else {
                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < attr.size(); i++) {
                                    if (i > 0) sb.append(", ");
                                    sb.append(attr.get(i));
                                }
                                row.put(attrName, sb.toString());
                            }
                        }
                    }

                    results.add(row);

                    if (results.size() >= maxResults && maxResults > 0) {
                        break;
                    }
                } catch (NamingException e) {
                    // Log but continue - some entries may have access issues
                    LOG.log(Level.WARNING, "Error reading search result: " + e.getMessage());
                }
            }

        } catch (javax.naming.SizeLimitExceededException e) {
            // This is OK - we hit the size limit but still got results
            LOG.log(Level.WARNING, "Size limit exceeded, returning " + results.size() + " results");
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException e) {
                    // Ignore
                }
            }
        }

        return results;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        if (currentResultSet != null) {
            currentResultSet.close();
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        return queryMaxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.queryMaxRows = max;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    // Required Statement interface methods - minimal implementations

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("LDAP does not support updates");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException { return 0; }
    @Override
    public void setMaxFieldSize(int max) throws SQLException { }
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException { }
    @Override
    public void cancel() throws SQLException { }
    @Override
    public SQLWarning getWarnings() throws SQLException { return null; }
    @Override
    public void clearWarnings() throws SQLException { }
    @Override
    public void setCursorName(String name) throws SQLException { }
    @Override
    public int getUpdateCount() throws SQLException { return -1; }
    @Override
    public boolean getMoreResults() throws SQLException { return false; }
    @Override
    public void setFetchDirection(int direction) throws SQLException { }
    @Override
    public int getFetchDirection() throws SQLException { return ResultSet.FETCH_FORWARD; }
    @Override
    public void setFetchSize(int rows) throws SQLException { }
    @Override
    public int getFetchSize() throws SQLException { return 0; }
    @Override
    public int getResultSetConcurrency() throws SQLException { return ResultSet.CONCUR_READ_ONLY; }
    @Override
    public int getResultSetType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override
    public void addBatch(String sql) throws SQLException { }
    @Override
    public void clearBatch() throws SQLException { }
    @Override
    public int[] executeBatch() throws SQLException { return new int[0]; }
    @Override
    public boolean getMoreResults(int current) throws SQLException { return false; }
    @Override
    public ResultSet getGeneratedKeys() throws SQLException { return null; }
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { return 0; }
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { return 0; }
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException { return 0; }
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { return execute(sql); }
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException { return execute(sql); }
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException { return execute(sql); }
    @Override
    public int getResultSetHoldability() throws SQLException { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override
    public void setPoolable(boolean poolable) throws SQLException { }
    @Override
    public boolean isPoolable() throws SQLException { return false; }
    @Override
    public void closeOnCompletion() throws SQLException { }
    @Override
    public boolean isCloseOnCompletion() throws SQLException { return false; }
    @Override
    public long getLargeUpdateCount() throws SQLException { return -1; }
    @Override
    public void setLargeMaxRows(long max) throws SQLException { queryMaxRows = (int) max; }
    @Override
    public long getLargeMaxRows() throws SQLException { return queryMaxRows; }
    @Override
    public long[] executeLargeBatch() throws SQLException { return new long[0]; }
    @Override
    public long executeLargeUpdate(String sql) throws SQLException { return 0; }
    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException { return 0; }
    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException { return 0; }
    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException { return 0; }
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
