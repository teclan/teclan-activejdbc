package teclan.activejdbc.service;

import java.io.File;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javalite.activejdbc.DB;
import org.javalite.activejdbc.RowListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.db.Database;
import teclan.activejdbc.model.DbField;
import teclan.activejdbc.model.DbRecord;
import teclan.utils.FileUtils;

public abstract class DefaultDbService implements DbService {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private static final String EVENS_TABLE                           = "EVENTS_TABLE";
    private static final String EVENS_TABLE_SEQ                       = "EVENTS_TABLE_SEQ";
    private static final String DROP_TRIGGER_EVENT_TABLE_SQL_TEMPLATE = "DROP TABLE %s.%s";
    private static final String DROP_TRIGGER_SQL_TEMPLATE             = "DROP TRIGGER %s.%s";
    private static final String SELECT_SQL_TEMPLATE                   = "SELECT * FROM %s";
    private static final String SELECT_SQL_WITH_WHERE_CLAUSE_TEMPLATE = "SELECT * FROM %s WHERE %s";

    private static final String[] TRIGGER_ACTIONS = { "INSERT", "UPDATE",
            "DELETE" };
    private static boolean        rebuild_trigger = true;

    private DataSource dataSource;

    private Database database;

    private String name = "default";

    private RetrieverListener listener;

    // <tableName, <columnName, columnDataType>>
    private Map<String, Map<String, String>> columnTypeMap = null;
    // <uppercasedDataType, Datatype>
    private Map<String, DataType>            dbDataTypes   = null;

    public DefaultDbService(String name, String type, String host, int port,
            String dbName, String schema, String user, String password) {
        this.dataSource = new DataSource(type, host, port, dbName, schema, user,
                password, getDriverClass(), getUrlTemplate());

        this.name = name;

        init();
    }

    public DefaultDbService(String name, DataSource dataSource) {

        dataSource.setDriverClass(getDriverClass());
        dataSource.setUrlTemplate(getUrlTemplate());

        this.dataSource = dataSource;
        this.name = name;

        init();
    }

    private void init() {

        database = new Database(dataSource);

        database.initDb();
        columnTypeMap = new HashMap<String, Map<String, String>>();

    }

    /**
     * @author Teclan
     * 
     *         是否存在名为 tableName 的表
     * 
     */
    @SuppressWarnings("rawtypes")
    public boolean hasTable(String tableName) {
        List<Map> result = new DB(name).findAll(getSelectTable(tableName));
        return !result.isEmpty();
    }

    /**
     * @author Teclan
     * 
     *         创建事件记录表,用于触发器
     * 
     */
    public void createEventTable() {
        new DB(getName()).exec(getCreateEventTableSql());
    }

    /**
     * @author Teclan
     * 
     *         删除事件记录表
     */
    public void dropEventTable() {
        if (hasTable(getEventTableName())) {
            new DB(name).exec(getDropEventTableSql());
        }
    }

    protected String getDropEventTableSql() {
        String dropTableSql = String.format(
                DROP_TRIGGER_EVENT_TABLE_SQL_TEMPLATE,
                getSchema().toUpperCase(), getEventTableName());
        return dropTableSql;
    }

    /**
     * @author Teclan
     * 
     *         创建触发器,通过 rebuildTrigger() 设置是否需要重建触发器
     * 
     * @param tableName
     * 
     *            触发器作用的对象表
     */
    public void createTrigger(String tableName) {

        String createTriggerSql = "";

        if (rebuild_trigger) {

            for (String action : TRIGGER_ACTIONS) {

                if (Triggerexists(tableName, action)) {
                    new DB(this.name).exec(getDropTriggerSql(
                            getTriggerName(tableName, action)));
                }

                createTriggerSql = getCreateTriggerSql(tableName, action);
                if (createTriggerSql != null) {
                    try {
                        new DB(this.name).exec(createTriggerSql);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                } else {
                    LOGGER.error("table " + dataSource.getSchema() + "."
                            + tableName + " doesn't has pk,trigger for "
                            + action + " create faild !!!!!");
                }
            }

        } else {

            for (String action : TRIGGER_ACTIONS) {
                if (!Triggerexists(tableName, action)) {
                    createTriggerSql = getCreateTriggerSql(tableName, action);
                    if (createTriggerSql != null) {
                        new DB(this.name).exec(createTriggerSql);
                    } else {
                        LOGGER.error("table " + dataSource.getSchema() + "."
                                + tableName + " doesn't has pk,trigger for "
                                + action + " create faild !!!!!");

                    }
                }
            }

        }
    }

    /**
     * @author Teclan
     * 
     *         移除触发器 tableName 上的触发器
     * @param tableName
     * 
     */
    public void removeTrigger(String tableName) {
        for (String action : TRIGGER_ACTIONS) {
            if (Triggerexists(tableName, action)) {
                new DB(name).exec(
                        getDropTriggerSql(getTriggerName(tableName, action)));
            }
        }
    }

    /**
     * @author Decaln
     * 
     *         是否存在触发器
     */
    @SuppressWarnings("rawtypes")
    public boolean Triggerexists(String tableName, String action) {
        List<Map> result = new DB(name)
                .findAll(getSelectTriggerSql(tableName, action));
        return !result.isEmpty();
    }

    /**
     * @author Teclan
     * 
     *         获取当前用户下的所有表名称
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("rawtypes")
    public ArrayList<String> getTables() throws SQLException {
        ArrayList<String> names = null;

        if (hasConnected()) {
            closeDatabase();
        }

        new DB(getName()).open(getDriverClass(), getUrl(), getUsername(),
                dataSource.getPassword());

        List<Map> result = new DB(getName()).findAll(buildQueryTablesSql());

        names = new ArrayList<String>();
        for (Map row : result) {
            names.add(row.get(tableNameColumn()).toString());
        }
        closeDatabase();

        return names;
    }

    /**
     * @author Teclan
     * 
     *         获取表 table 上的数据
     */
    public void retrieve(final String table) {
        retrieve(table, "INSERT", null, null, null);
    }

    /**
     * @author Teclan
     * 
     *         获取表 table 上的数据,pkNames为null是获取所有记录
     */
    public void retrieve(final String table, final String action,
            final String[] pkNames, final Object[] newPkValues,
            final Object[] oldPkValues) {

        // 全表同步
        if (pkNames == null) {

            final String[] pkNamesArray = getPkNamesArray(table);

            getTableColumns(table);

            new DB(name).find("select * from " + getTableNameWithSchema(table))
                    .with(new RowListenerAdapter() {
                        @Override
                        public void onNext(Map<String, Object> row) {
                            listener.recordRetrieved(createDbRecord(table,
                                    action, row, pkNamesArray));
                        }
                    });

            columnTypeMap.remove(table);
            return;
        }

        String query = getQueryString(pkNames);

        if (hasRecord(table, query, newPkValues)) {
            columnTypeMap.put(table, getTableColumns(table));
            new DB(name)
                    .find(getSelectSql(table, query),
                            getQueryParams(newPkValues))
                    .with(new RowListenerAdapter() {

                        @Override
                        public void onNext(Map<String, Object> row) {
                            listener.recordRetrieved(
                                    createDbRecord(table, action, row, pkNames,
                                            newPkValues, oldPkValues));
                        }

                    });
            columnTypeMap.remove(table);
        }

    }

    /**
     * @author Teclan
     * 
     *         获取表带schema的表名
     */
    protected String getTableNameWithSchema(String tableName) {
        return String.format("\"%s\".\"%s\"", getSchema(), tableName);
    }

    /**
     * @author Teclan
     * 
     *         获取表 table 上的数据
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void retrieveWithoutAdapter(final String table, final String action,
            String[] pkNames, Object[] newPkValues, Object[] oldPkValues) {

        String query = getQueryString(pkNames);

        if (hasRecord(table, query, newPkValues)) {

            getTableColumns(table);

            String SQL = "select * from %s.\"%s\" where %s";
            List<Map> rows = new DB(name).findAll(
                    String.format(SQL, dataSource.getSchema(), table, query),
                    newPkValues);

            listener.recordRetrieved(createDbRecord(table, action, rows.get(0),
                    pkNames, newPkValues, oldPkValues));

            columnTypeMap.remove(table);

        }

    }

    private boolean hasRecord(String table, String query, Object... params) {
        return new DB(name).count(getTableNameWithSchema(table),
                (query == null ? "*" : query), getQueryParams(params)) > 0;
    }

    protected Object[] getQueryParams(Object[] params) {
        return params;
    }

    private String getSelectSql(String table, String query) {
        String template = SELECT_SQL_TEMPLATE;
        if (query != null) {
            template = SELECT_SQL_WITH_WHERE_CLAUSE_TEMPLATE;
        }
        return String.format(template, getTableNameWithSchema(table), query);
    }

    private DbRecord createDbRecord(String table, String action,
            Map<String, Object> row, String[] pkNamesArray) {

        return createDbRecord(table, action, row, pkNamesArray, null, null);
    }

    private DbRecord createDbRecord(String table, String action,
            Map<String, Object> row, String[] pkNames, Object[] newPkValues,
            Object[] oldPkValues) {

        List<String> pkNamesList = new ArrayList<String>();
        List<DbField> pkFields = new ArrayList<DbField>();
        List<DbField> dbFields = new ArrayList<DbField>();

        if ("UPDATE".equals(action.toUpperCase())) {

            // 构造 PKFields
            for (int i = 0; i < pkNames.length; i++) {
                String pk = pkNames[i];
                pkNamesList.add(pk);
                String columnTypeKey = columnTypeMap.get(table)
                        .get(pk.toUpperCase()).toUpperCase();
                DataType type = getDBDataTypes()
                        .get(columnTypeKey.toUpperCase());
                if (type == null) {
                    LOGGER.error("Data type of {} not found in mapping.",
                            columnTypeKey);
                    type = DataType.STRING;
                }
                // update 操作应该传 update 之前的主键值，防止主键被修改造成的数据更新错误
                DbField field = new DbField(pk, oldPkValues[i], type);

                if (field.value == null) {
                    continue;
                }

                pkFields.add(field);
            }

            // 构造DBFields，
            // 对更新操作，dbFields中存有包括主键在内的所有最新属性和属性值，pkFields中存的是更新之前的主键值
            for (String key : row.keySet()) {
                String columnTypeKey = columnTypeMap.get(table)
                        .get(key.toUpperCase()).toUpperCase();
                DataType type = getDBDataTypes()
                        .get(columnTypeKey.toUpperCase());
                if (type == null) {
                    LOGGER.error("Data type of {} not found in mapping.",
                            columnTypeKey);
                    type = DataType.STRING;
                }
                DbField field = new DbField(key, row.get(key), type);

                if (field.value == null) {
                    continue;
                }
                dbFields.add(field);
            }

        } else {

            // 构造 PKFilds
            for (int i = 0; i < pkNames.length; i++) {
                String pk = pkNames[i];
                pkNamesList.add(pk);
                String columnTypeKey = columnTypeMap.get(table)
                        .get(pk.toUpperCase()).toUpperCase();
                DataType type = getDBDataTypes()
                        .get(columnTypeKey.toUpperCase());
                if (type == null) {
                    LOGGER.error("Data type of {} not found in mapping.",
                            columnTypeKey);
                    type = DataType.STRING;
                }
                DbField field = new DbField(pk, newPkValues[i], type);
                pkFields.add(field);
            }

            // 构造 DBFilds
            for (String key : row.keySet()) {
                if (!pkNamesList.contains(key)) {
                    String columnTypeKey = columnTypeMap.get(table)
                            .get(key.toUpperCase()).toUpperCase();
                    DataType type = getDBDataTypes().get(columnTypeKey);
                    if (type == null) {
                        LOGGER.error("Data type of {} not found in mapping.",
                                columnTypeKey);
                        type = DataType.STRING;
                    }
                    DbField field = new DbField(key, row.get(key), type);
                    dbFields.add(field);
                }
            }
        }

        return new DbRecord(dataSource.getDbName(), table, action, pkFields,
                dbFields);
    }

    /**
     * @author Teclan
     * 
     *         获取表结构
     * 
     * @param tableName
     * @return 字段名,数据类型
     */
    public Map<String, String> getTableColumns(String tableName) {
        Map<String, Map<String, String>> map = getColumnTypeMap();

        final Map<String, String> columnMap = new HashMap<String, String>();

        if (map.get(tableName) == null) {

            map.put(tableName, columnMap);

            new DB(getName()).find(getSelectColumnsSql(tableName))
                    .with(new RowListenerAdapter() {
                        @Override
                        public void onNext(Map<String, Object> row) {

                            columnMap.put(
                                    row.get("column_name").toString()
                                            .toUpperCase(),
                                    row.get("data_type").toString());
                        }
                    });
        }

        return columnMap;
    }

    /**
     * @author Teclan
     * 
     *         获取表主键名
     * @param table
     * @return 主键名
     */
    @SuppressWarnings("rawtypes")
    public List<String> getPkNames(String table) {
        List<Map> result = new DB(name).findAll(getSelectPkNamesSql(table));
        List<String> names = new ArrayList<String>();

        for (Map row : result) {
            names.add(row.get(getPkColumnName()).toString());
        }

        return names;
    }

    @SuppressWarnings("rawtypes")
    protected String[] getPkNamesArray(String table) {
        List<Map> result = new DB(name).findAll(getSelectPkNamesSql(table));
        String[] names = new String[result.size()];

        for (int i = 0; i < result.size(); i++) {
            names[i] = result.get(i).get(getPkColumnName()).toString();
        }
        return names;
    }

    private Map<String, Map<String, String>> getColumnTypeMap() {
        return columnTypeMap;
    }

    protected String getQueryString(String[] pkNames) {
        String[] quotedPkNames = new String[pkNames.length];
        for (int i = 0; i < pkNames.length; i++) {
            quotedPkNames[i] = String.format("\"%s\"", pkNames[i]);
        }

        return String.join(" = ? and ", quotedPkNames) + " = ?";
    }

    public boolean openDatabase() {
        return database.openDatabase();
    }

    public void closeDatabase() {
        database.closeDatabase();
    }

    public boolean openDatabase(String name) {
        return database.openDatabase(name);
    }

    public void closeDatabase(String name) {
        database.closeDatabase(name);
    }

    public boolean hasConnected(String name) {
        return new DB(name).hasConnection();
    }

    public boolean hasConnected() {
        return this.hasConnected("default");
    }

    protected String getUrl() {
        return dataSource.getUrl();
    }

    protected String getSchema() {
        return dataSource.getSchema();
    }

    protected String getUsername() {
        return dataSource.getUser();
    }

    protected String getPassword() {
        return dataSource.getPassword();
    }

    protected String getName() {
        return name;
    }

    protected String getDbName() {
        return dataSource.getDbName();
    }

    protected String getEventTableName() {
        return EVENS_TABLE;
    }

    protected String getEventTableIdSeq() {
        return EVENS_TABLE_SEQ;
    }

    protected String getDropTriggerSql(String triggerName) {
        return String.format(DROP_TRIGGER_SQL_TEMPLATE, getSchema(),
                triggerName);
    }

    @SuppressWarnings("rawtypes")
    protected ArrayList<String> getFKConstraint(String tableName) {
        ArrayList<String> names = new ArrayList<String>();
        try {
            List<Map> result = new DB(getName())
                    .findAll(buildQueryFKConstraintSql(tableName));
            for (Map row : result) {
                names.add(row.get(fkContraintNameColumn()).toString());
            }
        } catch (Exception e) {
            LOGGER.error("get FK constraint failed: " + e.getMessage());
        }
        return names;
    }

    /**
     * @author Teclan
     * 
     *         处理 DbRecord 对象
     * @param record
     * @throws Exception
     */
    @SuppressWarnings({ "unchecked", "unchecked" })
    public void handle(DbRecord record) throws Exception {
        try {
            // disableFkConstraint(record.getTableName());

            String action = record.getAction().toLowerCase();
            if (action.equals("insert")) {
                new DB(name).exec(generateInsertSql(record), generateParams(
                        record.getPkFields(), record.getDbFields()));
            } else if (action.equals("update")) {
                new DB(name).exec(generateUpdateSql(record), generateParams(
                        record.getDbFields(), record.getPkFields()));
            } else if (action.equals("delete")) {
                new DB(name).exec(generateDeleteSql(record),
                        generateParams(record.getPkFields()));
            } else {
                LOGGER.error("Unknown operation!");
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new Exception(e);
        } finally {
            // enableFkConstraint(record.getTableName());
        }
    }

    protected Object[] generateParams(List<DbField>... lists) {
        int fieldsLength = 0;
        for (List<DbField> list : lists) {
            fieldsLength += list.size();
        }

        Object[] params = (Object[]) Array.newInstance(Object.class,
                fieldsLength);
        int index = 0;
        for (List<DbField> list : lists) {
            for (DbField field : list) {
                Array.set(params, index++, field.getRealValue());
            }
        }

        return params;
    }

    protected String getExecuteSql(DbRecord dbRecord) {
        String action = dbRecord.getAction().toLowerCase();

        if (action.equals("insert")) {
            return generateInsertSql(dbRecord);
        } else if (action.equals("update")) {
            return generateUpdateSql(dbRecord);
        } else if (action.equals("delete")) {
            return generateDeleteSql(dbRecord);
        } else {
            return "";
        }
    }

    protected String generateInsertSql(DbRecord dbRecord) {

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                getTableNameWithSchema(dbRecord),
                generateColumnsString(dbRecord),
                generatePlaceHolder(dbRecord.getFieldLength()));
    }

    protected String generateUpdateSql(DbRecord dbRecord) {
        String conditionStringTemplate = quotedColumnName() ? "\"%s\" = ?"
                : "%s = ?";

        List<String> conditionStrings = new ArrayList<String>(
                dbRecord.getPkFields().size());
        for (DbField field : dbRecord.getPkFields()) {
            conditionStrings.add(
                    String.format(conditionStringTemplate, field.getKey()));
        }

        List<String> updateStrings = new ArrayList<String>(
                dbRecord.getDbFields().size());
        for (DbField field : dbRecord.getDbFields()) {
            updateStrings.add(
                    String.format(conditionStringTemplate, field.getKey()));
        }

        return String.format("update %s set %s where %s",
                getTableNameWithSchema(dbRecord),
                Joiner.on(", ").join(updateStrings),
                Joiner.on(" and ").join(conditionStrings));
    }

    protected String generateDeleteSql(DbRecord dbRecord) {
        String conditionStringTemplate = quotedColumnName() ? "\"%s\" = ?"
                : "%s = ?";

        List<String> conditionStrings = new ArrayList<String>(
                dbRecord.getPkFields().size());
        for (DbField field : dbRecord.getPkFields()) {
            conditionStrings.add(
                    String.format(conditionStringTemplate, field.getKey()));
        }

        return String.format("delete from %s where %s",
                getTableNameWithSchema(dbRecord),
                Joiner.on(" and ").join(conditionStrings));
    }

    protected String getTableNameWithSchema(DbRecord dbRecord) {
        String template = quotedTableName() ? "\"%s\".\"%s\"" : "%s.%s";

        return String.format(template, dataSource.getSchema(),
                dbRecord.getTableName());
    }

    protected String generateColumnsString(DbRecord dbRecord) {
        List<String> columns = new ArrayList<String>();

        for (DbField field : dbRecord.getPkFields()) {
            columns.add(generateColumnName(field.getKey()));
        }

        for (DbField field : dbRecord.getDbFields()) {
            columns.add(generateColumnName(field.getKey()));
        }

        return Joiner.on(",").join(columns);
    }

    protected String generatePlaceHolder(int length) {
        return generatePlaceHolder("?", length);
    }

    protected String generatePlaceHolder(String placeholder, int length) {
        List<String> list = new ArrayList<String>(length);
        for (int i = 0; i < length; i++) {
            list.add(placeholder);
        }

        return Joiner.on(',').join(list);
    }

    protected String generateColumnName(String key) {
        if (quotedColumnName()) {
            return String.format("\"%s\"", key);
        }

        return key;
    }

    protected boolean quotedColumnName() {
        return true;
    }

    protected boolean quotedTableName() {
        return true;
    }

    /**
     * @author Teclan
     * 
     *         禁用外键约束
     * @param tableName
     */
    public void disableFkConstraint(String tableName) {
        ArrayList<String> fk = getFKConstraint(tableName);

        if (fk.isEmpty()) {
            return;
        }

        for (String key : fk) {
            new DB(getName()).exec(
                    String.format(getDisableFkConstraintSql(), tableName, key));
        }
    }

    /**
     * @author Teclan
     * 
     *         启用外键约束
     * @param tableName
     */
    public void enableFkConstraint(String tableName) {

        ArrayList<String> foreignKeys = getFKConstraint(tableName);

        if (foreignKeys.isEmpty()) {
            return;
        }

        for (String key : foreignKeys) {
            new DB(getName()).exec(
                    String.format(getEnableFkConstraintSql(), tableName, key));
        }
    }

    public Map<String, DataType> getDBDataTypes() {
        if (dbDataTypes == null) {
            dbDataTypes = new HashMap<String, DataType>();

            dbDataTypes.put("INT", DataType.INTEGER);
            dbDataTypes.put("SMALLINT", DataType.INTEGER);
            dbDataTypes.put("TINYINT", DataType.INTEGER);
            dbDataTypes.put("INTEGER", DataType.INTEGER);

            // mysql
            dbDataTypes.put("YEAR", DataType.INTEGER);

            dbDataTypes.put("BIGINT", DataType.LONG);
            dbDataTypes.put("LONG", DataType.LONG);

            dbDataTypes.put("FLOAT", DataType.FLOAT);

            dbDataTypes.put("NUMERIC", DataType.NUMBER);
            dbDataTypes.put("NUMBER", DataType.NUMBER);
            dbDataTypes.put("DEC", DataType.NUMBER);

            dbDataTypes.put("DECIMAL", DataType.DOUBLE);
            dbDataTypes.put("REAL", DataType.DOUBLE);
            dbDataTypes.put("SINGLE", DataType.DOUBLE);
            dbDataTypes.put("DOUBLE", DataType.DOUBLE);
            dbDataTypes.put("MONEY", DataType.DOUBLE);
            dbDataTypes.put("SMALLMONEY", DataType.DOUBLE);
            dbDataTypes.put("DOUBLE PRECISION", DataType.DOUBLE);

            dbDataTypes.put("BINARY_FLOAT", DataType.DOUBLE);
            dbDataTypes.put("BINARY_DOUBLE", DataType.DOUBLE);

            dbDataTypes.put("CHAR", DataType.STRING);
            dbDataTypes.put("NCHAR", DataType.STRING);
            dbDataTypes.put("TINYTEXT", DataType.STRING);
            dbDataTypes.put("LONGTEXT", DataType.STRING);
            dbDataTypes.put("VARCHAR", DataType.STRING);
            dbDataTypes.put("NVARCHAR", DataType.STRING);
            dbDataTypes.put("VARCHAR2", DataType.STRING);
            dbDataTypes.put("NVARCHAR2", DataType.STRING);
            dbDataTypes.put("CHARACTER", DataType.STRING);
            dbDataTypes.put("CHARACTER VARYING", DataType.STRING);
            // DM
            dbDataTypes.put("INTERVAL YEAR", DataType.STRING);
            dbDataTypes.put("INTERVAL YEAR TO MONTH", DataType.STRING);
            dbDataTypes.put("INTERVAL MONTH", DataType.STRING);
            dbDataTypes.put("INTERVAL DAY", DataType.STRING);
            dbDataTypes.put("INTERVAL DAY TO HOUR", DataType.STRING);
            dbDataTypes.put("INTERVAL DAY TO MINUT", DataType.STRING);
            dbDataTypes.put("INTERVAL DAY TO SECOND", DataType.STRING);
            dbDataTypes.put("INTERVAL HOUR", DataType.STRING);
            dbDataTypes.put("INTERVAL HOUR TO MINUTE", DataType.STRING);
            dbDataTypes.put("INTERVAL HOUR TO SECOND", DataType.STRING);
            dbDataTypes.put("INTERVAL MINUTE", DataType.STRING);
            dbDataTypes.put("INTERVAL MINUTE TO SECOND", DataType.STRING);
            dbDataTypes.put("INTERVAL SECOND", DataType.STRING);

            dbDataTypes.put("BOOL", DataType.BOOLEAN);
            dbDataTypes.put("BOOLEAN", DataType.BOOLEAN);

            dbDataTypes.put("TIME", DataType.DATETIME);
            dbDataTypes.put("DATE", DataType.DATETIME);
            dbDataTypes.put("DATETIME", DataType.DATETIME);
            dbDataTypes.put("DATETIME2", DataType.DATETIME);
            dbDataTypes.put("SMALLDATETIME", DataType.DATETIME);
            dbDataTypes.put("TIMESTAMP", DataType.DATETIME);
            /* ORACLE */
            dbDataTypes.put("TIMESTAMP(6)", DataType.DATETIME);
            dbDataTypes.put("TIMESTAMP(6) WITH TIME ZONE", DataType.DATETIME);
            dbDataTypes.put("TIMESTAMP(6) WITH LOCAL TIME ZONE",
                    DataType.DATETIME);
            /* DM */
            dbDataTypes.put("TIMESTAMP WITH LOCAL TIME ZONE",
                    DataType.DATETIME);

            /* ORACLE */
            dbDataTypes.put("TEXT", DataType.CLOB);
            dbDataTypes.put("NTEXT", DataType.CLOB);
            dbDataTypes.put("MEMO", DataType.CLOB);
            dbDataTypes.put("CLOB", DataType.CLOB);
            dbDataTypes.put("NCLOB", DataType.CLOB);
            dbDataTypes.put("DBCLOB", DataType.CLOB);
            dbDataTypes.put("CHARACTER LARGE OBJECT", DataType.CLOB);

            dbDataTypes.put("BINARY LARGE OBJECT", DataType.BLOB);
            dbDataTypes.put("BYTE", DataType.BLOB);
            dbDataTypes.put("RAW", DataType.BLOB);
            dbDataTypes.put("LONG RAW", DataType.BLOB);
            dbDataTypes.put("BLOB", DataType.BLOB);
            dbDataTypes.put("BFILE", DataType.BLOB);
            // dbDataTypes.put("BINARY", DataType.BLOB);
            dbDataTypes.put("VARBINARY", DataType.BLOB);
            dbDataTypes.put("IMAGE", DataType.BLOB);
        }

        if ("kingbase".equals(getDbType())) {
            // kingbase
            dbDataTypes.put("BIT", DataType.BOOLEAN);
            dbDataTypes.put("BIT VARYING", DataType.STRING);
        } else if ("dm".equals(getDbType())) {
            dbDataTypes.put("BINARY", DataType.STRING);
            dbDataTypes.put("VARBINARY", DataType.STRING);
            dbDataTypes.put("BIT", DataType.INTEGER);
        } else if ("mysql".equals(getDbType())) {
            dbDataTypes.put("BIT", DataType.INTEGER);
            dbDataTypes.put("BINARY", DataType.COMMON_BINARY);
            dbDataTypes.put("VARBINARY", DataType.COMMON_BINARY);

            // 包括sqlserver2000,sqlserver2000+
        } else if (getDbType().getValue().toLowerCase().contains("sqlserver")) {
            dbDataTypes.put("BINARY", DataType.COMMON_BINARY);
        }

        return dbDataTypes;
    }

    /**
     * @author Teclan
     * 
     *         执行 sql 脚本
     * 
     * @param sql
     */
    public void execute(String sql) {
        this.execute(getName(), sql);
    }

    /**
     * @author Teclan
     * 
     *         指定连接名并执行 sql 脚本
     * 
     * @param sql
     */
    public void execute(String name, String sql) {

        try {
            new DB(name).exec(sql);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * @author Teclan
     * 
     *         读取文件,并执行内容脚本,如果一个文件中存在多组脚本,每组脚本之间用"#"隔开,
     * 
     *         形如: create table tableName (....); # insert into tableName(...)
     *         values (...);#
     * 
     * @param file
     *            脚本文件
     */
    public void execute(File file) {

        this.execute(getName(), file);
    }

    /**
     * @author Teclan
     * 
     *         指定连接名,读取文件,并执行内容脚本,如果一个文件中存在多组脚本,每组脚本之间用"#"隔开,
     * 
     *         形如: create table tableName (....); # insert into tableName(...)
     *         values (...);#
     * 
     * @param file
     *            脚本文件
     */
    public void execute(String name, File file) {

        String[] contents = FileUtils.getContent(file).split("#");

        if (contents == null) {
            LOGGER.warn("脚本内容为空!:{}", file.getAbsolutePath());
            return;
        }

        for (String sql : contents) {
            this.execute(name, sql);
        }

    }

    protected String getTriggerName(String tableName, String action) {
        return String.format("Teclan_%s_%s", tableName, action);
    }

    /**
     * 
     * @author Teclan
     * 
     *         是否需要全部重建触发器,默认值是 false,没有触发器的表也会创建触发器,
     * 
     *         如果值为true,则将原来存在的触发器删除再重建
     * 
     * @param rebuildTrigger
     */
    public void rebuildTrigger(boolean rebuildTrigger) {
        rebuild_trigger = rebuildTrigger;
    }

    public void createSequence() {
        // do nothing ,dm,oracle overwrite this method
    }

    /**
     * @author Teclan
     * 
     *         设置监听对象,用于从表中获取数据
     * @param listener
     */
    public void setRetrieverListener(RetrieverListener listener) {
        this.listener = listener;
    }

    public abstract String getDriverClass();

    public abstract String getUrlTemplate();

    public abstract DbType getDbType();

    protected abstract String tableNameColumn();

    protected abstract String buildQueryTablesSql();

    protected abstract String getSelectColumnsSql(String tableName);

    protected abstract String getSelectTable(String tableName);

    protected abstract String getCreateEventTableSql();

    protected abstract String getSelectTriggerSql(String tableName,
            String action);

    protected abstract String getCreateTriggerSql(String tableName,
            String action);

    protected abstract String getPkColumnName();

    protected abstract String getSelectPkNamesSql(String table);

    protected abstract String fkContraintNameColumn();

    protected abstract String buildQueryFKConstraintSql(String tableName);

    protected abstract String getDisableFkConstraintSql();

    protected abstract String getEnableFkConstraintSql();

}
