/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.sql;


import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.SqlClause;
import com.palantir.util.TextUtils;


public class SQLString extends BasicSQLString {
    private static final Pattern ALL_WORD_CHARS_REGEX = Pattern.compile("^[a-zA-Z_0-9\\.\\-]*$"); //$NON-NLS-1$

    /**
     * Callers changing the value of cachedUnregistered and
     * cachedKeyed should be synchronized on this lock. Readers
     * do not need to - the values of those maps are not guaranteed
     * to be in sync with each other.
     */
    private static final Object cacheLock = new Object();
    //TODO (DCohen): Combine cachedKeyed and cachedUnregistered maps into one.
    /**
     * Rewritten unregistered queries.
     * Key: String with all whitespace removed
     * Value: the new SQLString to run instead.
     */
    @GuardedBy("cacheLock")
    protected static volatile ImmutableMap<String, FinalSQLString> cachedUnregistered = ImmutableMap.of();
    /** Rewritten registered queries */
    @GuardedBy("cacheLock")
    protected static volatile ImmutableMap<String, FinalSQLString> cachedKeyed = ImmutableMap.of();
    /** All registered queries */
    protected static final ConcurrentMap<String, FinalSQLString> registeredValues = new ConcurrentHashMap<String, FinalSQLString>();
    /** DB-specific registered queries */
    protected static final ConcurrentMap<String, ConcurrentMap<DBType, FinalSQLString>> registeredValuesOverride = new ConcurrentHashMap<String, ConcurrentMap<DBType, FinalSQLString>>();

    protected static interface OnUseCallback {
        public void noteUse(SQLString used);
    }
    //by default, no callback. This is set in OverridableSQLString
    protected static OnUseCallback callbackOnUse = new OnUseCallback(){
        @Override
        public void noteUse(SQLString used) {
           //do nothing
        }};

    protected static interface CallableCheckedException<T, E extends Exception> {
        T call() throws E;
    }

    /**
     * Runs the provided callable while holding the lock for the override caches.
     * Callers replacing the caches should hold this lock.
     */
    protected <T, E extends Exception> T runWithCacheLock(CallableCheckedException<T, E> callable) throws E {
        synchronized (cacheLock) {
            return callable.call();
        }
    }

    /**
     * Call this function to store a query to be used later with the given key.
     * @param key Unique identifier for this query
     * @param sql The query that will be stored
     */
    public static RegisteredSQLString registerQuery(String key, String sql) {
        SQLString sqlString = new SQLString(key, sql, null);
        FinalSQLString newVal = new FinalSQLString(sqlString);
        FinalSQLString oldVal = registeredValues.put(key, newVal);
        assert null == oldVal || oldVal.delegate.equals(newVal.delegate) : "newVal: " + newVal + " oldVal: " + oldVal; //$NON-NLS-1$ //$NON-NLS-2$
        return new RegisteredSQLString(sqlString);
    }

    /**
     * Same as the overloaded registerQuery, but overrides the query for a specific DBType.
     * @param key Unique identifier representing this query
     * @param sql The query that will be stored
     * @param dbTypes Override the query for this list of DBTypes.  These are not allowed to be null.
     */
    public static void registerQuery(String key, String sql, DBType... dbTypes) {
        Validate.notEmpty(dbTypes, "DbType list may not be empty"); //$NON-NLS-1$
        for (DBType type : dbTypes) {
            Validate.notNull(type, "dbType must not be null"); //$NON-NLS-1$
            registerQuery(key, sql, type);
        }
    }

    /**
     * Same as the overloaded registerQuery, but overrides the query for a specific DBType.
     * @param key Unique identifier representing this query
     * @param sql The query that will be stored
     * @param dbType Override the query for this DBType.  If this value is null, it is the same as <code>registerQuery(key, sql)</code>
     */
    public static RegisteredSQLString registerQuery(String key, String sql, DBType dbType) {
        if (dbType == null) {
            return registerQuery(key, sql);
        }

        SQLString sqlString = new SQLString(key, sql, dbType);
        ConcurrentMap<DBType, FinalSQLString> newHash = new ConcurrentHashMap<DBType, FinalSQLString>();

        ConcurrentMap<DBType, FinalSQLString> dbTypeHash = registeredValuesOverride.putIfAbsent(key, newHash);
        if(null == dbTypeHash) {
            dbTypeHash = newHash;
        }
        FinalSQLString newVal = new FinalSQLString(sqlString);
        FinalSQLString oldVal = dbTypeHash.put(dbType, newVal);

        assert null == oldVal || newVal.delegate.equals(oldVal.delegate) : "newVal: " + newVal + " oldVal: " + oldVal; //$NON-NLS-1$ //$NON-NLS-2$
        return new RegisteredSQLString(sqlString);
    }

    public static boolean isQueryRegistered(String key) {
        return registeredValues.containsKey(key);
    }

    /**
     * A query that has been registered with <code>registerQuery</code> can be looked up by its key.
     * This factory returns a SQLString object representing the registered query.
     * The stored query may have been overridden in the database and the object returned will reflect that.
     * If the query is not overridden in the Database, we will check the dbType override first, then use the general registered query
     * This factory is used by <code>SQL</code> to find a registered query.
     *
     * @param key The key that was passed to <code>registerQuery</code>
     * @param dbType Look for queries registered with this override first
     * @return a SQLString object representing the stored query
     */
     static FinalSQLString getByKey(final String key, DBType dbType) {
        assert isValidKey(key) : "Keys only consist of word characters"; //$NON-NLS-1$
        assert registeredValues.containsKey(key) || registeredValuesOverride.containsKey(key) : "Couldn't find SQLString key: " + key + ", dbtype " + dbType; //$NON-NLS-1$ //$NON-NLS-2$

        FinalSQLString cached = cachedKeyed.get(key);
        if(null != cached) {
            callbackOnUse.noteUse((SQLString) cached.delegate);
            return cached;
        }

        ConcurrentMap<DBType, FinalSQLString> dbTypeHash = registeredValuesOverride.get(key);
        if(null != dbTypeHash) {
            FinalSQLString dbOverride = dbTypeHash.get(dbType);
            if(null != dbOverride) {
                return dbOverride;
            }
        }

        FinalSQLString valueForKey = registeredValues.get(key);
        if(valueForKey == null) {
            return new FinalSQLString(new NullSQLString(key));
        }
        return valueForKey;
     }

     static FinalSQLString getByKey(String key, Connection connection) throws PalantirSqlException {
         DBType type = DBType.getTypeFromConnection(connection);
         return getByKey(key, type);
     }

    public static boolean isValidKey(final String key) {
        return ALL_WORD_CHARS_REGEX.matcher(key).matches();
    }

    /**
     * A Factory used by the SQL class to turn a string sql query into an SQLString object.
     * This may just contain the sql given, or the given SQL may be overriden in the database and the object returned will reflect that new SQL from the DB.
     *
     * @param sql The string to be used in a query
     * @return a SQLString object representing the given SQL.
     */
    static FinalSQLString getUnregisteredQuery(String sql) {
        assert !isValidKey(sql) : "Unregistered Queries should not look like keys"; //$NON-NLS-1$
        FinalSQLString cached = cachedUnregistered.get(TextUtils.removeAllWhitespace(canonicalizeString(sql)));
        if(null != cached) {
            callbackOnUse.noteUse((SQLString) cached.delegate);
            return cached;
        }

        return new FinalSQLString(new SQLString(sql));
    }

    /**
     * Contructor for unregistered (dynamic) SQL
     * @param sql
     */
    private SQLString(String sql) {
        super(null, makeCommentString(null, null) + sql);
    }

    /**
     * Contructor for registered SQL
     * @param key
     * @param sql
     * @param dbType This is only used in making the SQL comment.
     */
    protected SQLString(String key, String sql, DBType dbType) {
        super(key, makeCommentString(key, dbType) + sql);
    }

    /**
     * Creates an appropriate comment string for the beginning of a SQL statement
     * @param keyString Identifier for the SQL; will be null if the SQL is unregistered
     * @param dbType
     * @param fromDB
     */
    private static String makeCommentString(String keyString, DBType dbType) {
        String registrationState;
        if(keyString != null) {
            registrationState = "SQLString Identifier: " + keyString; //$NON-NLS-1$
        } else {
            registrationState = "UnregisteredSQLString"; //$NON-NLS-1$
        }
        String dbTypeString = ""; //$NON-NLS-1$
        if(dbType != null)
         {
            dbTypeString = " dbType: " + dbType; //$NON-NLS-1$
        }
        String fromDBString = ""; //$NON-NLS-1$
        return "/* " + registrationState + dbTypeString + fromDBString + " */ "; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Cleans up whitespace, any trailing semicolon, and prefixed comments that a string is
     * unregistered, in order to come up with a canonical representation of this sql string.
     * @param s
     * @return
     */
    public static String canonicalizeString(String s) {
        String trim = s.trim();
        if(!trim.isEmpty() && trim.charAt(trim.length() - 1) == ';') {
            trim = trim.substring(0, trim.length() - 1);
        }
        String prefix = "/* UnregisteredSQLString */"; //$NON-NLS-1$
        if (trim.startsWith(prefix)) {
            trim = trim.substring(prefix.length()).trim();
        }
        String [] sp = trim.split("\\s+"); //$NON-NLS-1$
        return StringUtils.join(sp, " "); //$NON-NLS-1$
    }


    static class NullSQLString extends SQLString {
        final String key;
        public NullSQLString(String key) {
            super(""); //$NON-NLS-1$
            this.key = key;
        }

        @Override
        public String getQuery() {
            throw new PalantirRuntimeException("Could not find any registered query value for key: " + key +  //$NON-NLS-1$
                    "\nThe key is potentially an unregistered query."); //$NON-NLS-1$
        }
    }

    /** Routine for registering all the possible combinations of queries
     * given a set of keyed clauses. This is used to build up a map in
     * {@code map} from which clients can decode the queries string to use for
     * the clauses they want. The clauses will always occur in the generated
     * queries in the order listed the {@code clauses} array.
     *
     * @param baseKey the basic type of search
     * @param map a mapping from a set of restrictive clause names and the base
     * key to a distinguishing query name.
     * @param sqlFormat format string which takes one argument which is the
     * conjunction of clauses (from <code>clauses</code>) which modify the
     * query variant
     * @param baseClause the filter required for all instances of the search type
     * @param type database type the search is for, null for all DBs
     * @param keys keys for clauses that can be added to the search
     * @param clauses clauses (in the same order as their keys) which can narrow
     * the search
     */
    public static void registerQueryVariants(String baseKey,
                                             Map<Set<String>, String> map, String sqlFormat,
                                             DBType type, List<SqlClause> clauses) {
        Validate.noNullElements(clauses);
        Set<Integer> indexes = new HashSet<Integer>();
        for (int i = 0; i < clauses.size(); i++) {
            indexes.add(i);
        }

        Set<Set<Integer>> variants = Sets.powerSet(indexes);
        for (Set<Integer> variantSet : variants) {
            List<Integer> variant = new ArrayList<Integer>(variantSet);
            Collections.sort(variant);
            StringBuilder key = new StringBuilder(baseKey);
            StringBuilder whereClause = new StringBuilder();
            Set<String> keySet = new HashSet<String>();
            for (int i : variant) {
                SqlClause clause = clauses.get(i);
                keySet.add(clause.getKey());
                key.append("_" + clause.getKey()); //$NON-NLS-1$
                whereClause.append(" AND " + clause.getClause()); //$NON-NLS-1$
            }
            keySet.add(baseKey);
            String sql = String.format(sqlFormat, whereClause);
            String keyString = key.toString();
            registerQuery(keyString, sql, type);
            map.put(keySet, keyString);
        }
    }

    /**
     * Object returned when a query is registered.
     * Its only method is getKey(), because we can't actually rely on the SQL itself inside this
     * object (since it might be overridden).
     * @author dcohen
     *
     */
    public static class RegisteredSQLString {

        private final BasicSQLString delegate;

        /**
         * Should only be called inside SQLString because this class essentially verifies that we've
         * checked for updates.
         * @param delegate
         */
        private RegisteredSQLString(BasicSQLString sqlstring) {
            this.delegate = sqlstring;
        }


        @Override
        public String toString() {
            return "RegisteredSQLString [delegate=" + delegate + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        }

        public String getKey() {
            return delegate.getKey();
        }

    }

    public static RegisteredSQLString getRegisteredQueryByKey(FinalSQLString key) {
         return new RegisteredSQLString(key.delegate);
    }
}
