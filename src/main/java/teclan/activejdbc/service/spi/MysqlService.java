package teclan.activejdbc.service.spi;

import java.util.List;

import org.javalite.activejdbc.DB;

import com.google.common.base.Joiner;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.service.DbType;
import teclan.activejdbc.service.DefaultDbService;

public class MysqlService extends DefaultDbService {

    private static final String DRIVER_CLASS                 = "com.mysql.jdbc.Driver";
    private static final String URL_TEMPLATE                 = "jdbc:mysql://%s:%d/%s";
    private static final String SELECT_PK_NAMES_SQL_TEMPLATE = "select concat(c.column_name) as 'column_name' "
            + "from information_schema.table_constraints as t,"
            + "information_schema.key_column_usage as c "
            + "where t.table_name=c.table_name and t.table_name='%s' "
            + "and t.table_schema='%s'";

    private static final String SELECT_COLUMNS_SQL_TEMPLATE = "select column_name,data_type from information_schema.columns "
            + "where table_schema='%s' and table_name='%s'";
    private static final String QUERY_TABLES_SQL            = "select table_name from information_schema.tables where table_schema  ='%s'";

    private static final String SELECT_TABLE_SQL_TEMPLATE = "SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='%s'";

    private static final String CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE = "CREATE TABLE %s.%s "
            + "(id         BIGINT        NOT NULL PRIMARY KEY     AUTO_INCREMENT,"
            + " dbName     VARCHAR(30)," + " tableName  VARCHAR(30),"
            + " pkNames    VARCHAR(2000)," + " newPkValues   VARCHAR(2000),"
            + " oldPkValues   VARCHAR(2000)," + " action     VARCHAR(20))";

    private static final String SELECT_TRIGGER_SQL_TEMPLATE = "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='%s' "
            + "AND EVENT_OBJECT_TABLE='%s' " + "AND TRIGGER_NAME='%s'";

    private static final String CREATE_TRIGGER_SQL_TEMPLATE = "CREATE TRIGGER %s.%s AFTER %s ON %s.%s "
            + "FOR EACH ROW INSERT INTO %s.%s (dbName,tableName,pkNames,newPkValues,oldPkValues,action) "
            + "VALUES('%s','%s','%s',(%s),(%s),'%s');";

    private static final String DISABLE_FK_CONSTRAINT_SQL = "set foreign_key_checks=0";
    private static final String ENABLE_FK_CONSTRAINT_SQL  = "set foreign_key_checks=1";

    public MysqlService(String name, String type, String host, int port,
            String dbName, String schema, String user, String password) {
        super(name, type, host, port, dbName, schema, user, password);
    }

    public MysqlService(String name, DataSource dataSource) {

        super(name, dataSource);
    }

    @Override
    public String getDriverClass() {
        return DRIVER_CLASS;
    }

    @Override
    public String getUrlTemplate() {
        return URL_TEMPLATE;
    }

    @Override
    public DbType getDbType() {
        return DbType.MYSQL;
    }

    @Override
    protected String getSelectPkNamesSql(final String table) {
        return String.format(SELECT_PK_NAMES_SQL_TEMPLATE, table, getSchema());
    }

    @Override
    protected String getPkColumnName() {
        return "column_name";
    }

    @Override
    protected String getSelectColumnsSql(String tableName) {
        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, getSchema(),
                tableName);
    }

    @Override
    protected String getTableNameWithSchema(String tableName) {
        return String.format("%s.%s", getSchema(), tableName);
    }

    @Override
    protected String getQueryString(String[] pkNames) {
        return Joiner.on(" = ? and ").join(pkNames) + " = ?";
    }

    @Override
    protected String buildQueryTablesSql() {
        return String.format(QUERY_TABLES_SQL, getSchema());
    }

    @Override
    protected String tableNameColumn() {
        return "table_name";
    }

    @Override
    protected String getCreateEventTableSql() {
        return String.format(CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE,
                getSchema(), getEventTableName());
    }

    @Override
    protected String getSelectTriggerSql(String tableName, String action) {

        return String.format(SELECT_TRIGGER_SQL_TEMPLATE, getSchema(),
                tableName, getTriggerName(tableName, action));
    }

    @Override
    protected String getCreateTriggerSql(String tableName, String action) {
        List<String> pkNames = getPkNames(tableName);

        if (pkNames.isEmpty()) {
            return null;
        }
        String newPkValues = null;
        String oldPkValues = null;

        String pkNamesString = Joiner.on(",").join(pkNames);

        if ("DELETE".equalsIgnoreCase(action)) {
            oldPkValues = getPKContentsExpression(pkNames, "old");

        } else if ("UPDATE".equalsIgnoreCase(action)) {
            newPkValues = getPKContentsExpression(pkNames, "new");
            oldPkValues = getPKContentsExpression(pkNames, "old");

        } else if ("INSERT".equalsIgnoreCase(action)) {
            newPkValues = getPKContentsExpression(pkNames, "new");
        }

        String schema = getSchema();
        return String.format(CREATE_TRIGGER_SQL_TEMPLATE, schema,
                getTriggerName(tableName, action), action, schema, tableName,
                schema, getEventTableName(), getDbName(), tableName,
                pkNamesString, newPkValues, oldPkValues, action);
    }

    private String getPKContentsExpression(List<String> pkNames,
            String prefix) {

        String pkValuesSql = "concat_ws(',',%s)";
        String[] prefixedPkNames = new String[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            prefixedPkNames[i] = String.format("%s.%s", prefix, pkNames.get(i));
        }

        return String.format(pkValuesSql, Joiner.on(",").join(prefixedPkNames));

    }

    @Override
    protected String getSelectTable(String tableName) {
        return String.format(SELECT_TABLE_SQL_TEMPLATE, tableName);
    }

    @Override
    protected boolean quotedColumnName() {
        return false;
    }

    @Override
    protected boolean quotedTableName() {
        return false;
    }

    // Mysql不需要指定外键名就可以直接关闭所有外键约束
    @Override
    protected String buildQueryFKConstraintSql(String tableName) {
        return null;
    }

    @Override
    protected String fkContraintNameColumn() {
        return null;
    }

    @Override
    protected String getDisableFkConstraintSql() {
        return DISABLE_FK_CONSTRAINT_SQL;
    }

    @Override
    protected String getEnableFkConstraintSql() {
        return ENABLE_FK_CONSTRAINT_SQL;
    }

    @Override
    public void disableFkConstraint(String tableName) {
        new DB(getName()).exec(getDisableFkConstraintSql());

    }

    @Override
    public void enableFkConstraint(String tableName) {
        new DB(getName()).exec(getEnableFkConstraintSql());
    }

}
