package teclan.activejdbc.service.spi;

import java.util.List;

import org.javalite.activejdbc.DB;

import com.google.common.base.Joiner;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.model.DbRecord;
import teclan.activejdbc.service.DbType;
import teclan.activejdbc.service.DefaultDbService;
import teclan.activejdbc.utils.Strings;

public class Db2Service extends DefaultDbService {
    private static final String DRIVER_CLASS = "COM.ibm.db2os390.sqlj.jdbc.DB2SQLJDriver";
    private static final String URL_TEMPLATE = "jdbc:db2://%s:%s/%s";

    private static final String SELECT_PK_NAMES_SQL_TEMPLATE = "SELECT a.colname FROM SYSCAT.keycoluse AS a "
            + "INNER JOIN SYSCAT.tabconst AS b "
            + "ON (a.constname = b.constname) "
            + "WHERE a.tabschema = '%s' AND b.tabschema = '%s' "
            + "AND a.tabname = '%s' AND b.tabname = '%s' AND b.type = 'P'";

    private static final String SELECT_COLUMNS_SQL_TEMPLATE = "select column_name, data_type "
            + "from sysibm.columns " + "where table_name = upper('%s')";

    private static final String QUERY_TABLES_SQL = "SELECT name FROM SYSIBM.SYSTABLES WHERE creator='%s' ORDER BY name";

    private static final String SELECT_TABLE_SQL_TEMPLATE = "SELECT tabname FROM SYSCAT.tables "
            + "WHERE tabschema='%s' AND tabname='%s'";

    private static final String CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE = "CREATE TABLE %s.%s "
            + "(id INTEGER  NOT NULL GENERATED ALWAYS AS IDENTITY "
            + "(START WITH 0, INCREMENT BY 1, NO CACHE ) , "
            + "dbName VARCHAR(20), " + "tableName VARCHAR(20),  "
            + "pkNames VARCHAR(255), " + "newPkValues VARCHAR(255), "
            + "oldPkValues VARCHAR(255), " + "action VARCHAR(20), "
            + "PRIMARY KEY (id))";

    private static final String SELECT_TRIGGER_SQL_TEMPLATE = "SELECT trigname FROM SYSCAT.triggers "
            + "WHERE trigschema='%s' AND trigname='%s' "
            + "AND trigevent ='%s'";

    private static final String CREATE_TRIGGER_SQL_TEMPLATE = "CREATE TRIGGER %s.%s %s %s ON %s.%s "
            + "REFERENCING %s FOR EACH ROW MODE DB2SQL BEGIN ATOMIC "
            + "INSERT INTO %s.%s (dbName, tableName, pkNames, newPkValues, oldPkValues, action) "
            + "VALUES ('%s', '%s', '%s', %s, %s,'%s'); END";

    private static final String QUERY_FK_CONSTRAINT_SQL   = "select "
            + "r.reftabname as table_name, k.colname as column_name, r.constname as constraint_name "
            + "from syscat.references as r,syscat.keycoluse as k "
            + "where r.tabname=k.tabname and k.constname=r.constname and r.tanschema=’%s’ and r.reftabname='%s'";
    private static final String DISABLE_FK_CONSTRAINT_SQL = "alter table %s alter foreign key %s not enforced";
    private static final String ENABLE_FK_CONSTRAINT_SQL  = "alter table %s alter foreign key %s enforced";

    public Db2Service(String name, String type, String host, int port,
            String dbName, String schema, String user, String password) {
        super(name, type, host, port, dbName, schema, user, password);
    }

    public Db2Service(String name, DataSource dataSource) {

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
        return DbType.DB2;
    }

    @Override
    protected String getTableNameWithSchema(String tableName) {
        return String.format("%s.%s", getSchema(), tableName);
    }

    @Override
    protected String getSelectPkNamesSql(final String table) {
        String schema = getSchema().toUpperCase();

        return String.format(SELECT_PK_NAMES_SQL_TEMPLATE, schema, schema,
                table.toUpperCase(), table.toUpperCase());
    }

    @Override
    protected String getPkColumnName() {
        return "colname";
    }

    @Override
    protected String getSelectColumnsSql(String tableName) {
        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, tableName);
    }

    @Override
    protected String buildQueryTablesSql() {
        return String.format(QUERY_TABLES_SQL, getSchema().toUpperCase());
    }

    @Override
    protected String tableNameColumn() {
        return "name";
    }

    @Override
    protected String getCreateEventTableSql() {
        String schema = getSchema();
        return String.format(CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE, schema,
                getEventTableName());
    }

    @Override
    protected String getSelectTriggerSql(String tableName, String action) {
        return String.format(SELECT_TRIGGER_SQL_TEMPLATE,
                getSchema().toUpperCase(),
                getTriggerName(tableName.toUpperCase(), action),
                action.substring(0, 1).toUpperCase());
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
        String referencingSql = "";

        String schema = getSchema().toUpperCase();

        if (action.equals("INSERT")) {
            referencingSql = " NEW AS N ";
            newPkValues = getPKContentsExpression(pkNames, "N");

        } else if (action.equals("UPDATE")) {
            referencingSql = " NEW AS N OLD AS O ";
            newPkValues = getPKContentsExpression(pkNames, "N");
            oldPkValues = getPKContentsExpression(pkNames, "O");

        } else if ((action.equals("DELETE"))) {
            referencingSql = " OLD AS O ";
            oldPkValues = getPKContentsExpression(pkNames, "O");

        }

        return String.format(CREATE_TRIGGER_SQL_TEMPLATE, schema,
                getTriggerName(tableName, action), "AFTER", action, schema,
                tableName, referencingSql, schema, getEventTableName(),
                getDbName(), tableName, pkNamesString, newPkValues, oldPkValues,
                action);
    }

    /**
     * @author Declan
     * 
     *         DB2 数据库触发器名字不允许超过18个字符
     */
    @Override
    protected String getTriggerName(String tableName, String action) {

        return String.format("PX_%s_%s",
                Strings.toHexDigest(tableName, "SHA1").substring(0, 13),
                action.substring(0, 1)).toUpperCase();
    }

    private String getPKContentsExpression(List<String> pkNames,
            String prefix) {
        String[] prefixedPkNames = new String[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            prefixedPkNames[i] = String.format("CHAR(%s.%s)", prefix,
                    pkNames.get(i));
        }

        return Joiner.on("||','||").join(prefixedPkNames);
    }

    @Override
    protected String getSelectTable(String tableName) {
        return String.format(SELECT_TABLE_SQL_TEMPLATE, tableName);
    }

    // DB2数据库,存在以下问题:
    // 1:如果表中自增列使用的是 GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1,
    // NO CACHE)声明,则数据库将严格保证该列满足自增约束(唯一性),用户不可指定该列的值,无法插入数据.
    // 2:如果表中自增列使用的是 GENERATED BY DEFAULT AS IDENTITY (START WITH 0, INCREMENT
    // BY 1, NO CACHE)声明,则允许我们指定此列的值,对我们操作不影响.为此,用户建表的时候至少内网必须使用
    // GENERATED BY DEFAULT AS IDENTITY (START WITH 0, INCREMENT BY 1, NO
    // CACHE)作为自增声明方式

    @Override
    @SuppressWarnings("unchecked")
    public void handle(DbRecord record) throws Exception {
        try {
            // disableFkConstraint(record.getTableName());

            String action = record.getAction().toLowerCase();
            if (action.equals("insert")) {
                new DB(getName()).exec(generateInsertSql(record),
                        generateParams(record.getPkFields(),
                                record.getDbFields()));
            } else if (action.equals("update")) {
                new DB(getName()).exec(generateUpdateSql(record),
                        generateParams(record.getDbFields(),
                                record.getPkFields()));
            } else if (action.equals("delete")) {
                new DB(getName()).exec(generateDeleteSql(record),
                        generateParams(record.getPkFields()));
            } else {
                LOGGER.error("Unknown operation!");
            }

        } catch (Exception e) {
            LOGGER.error(
                    "SQL exceute faild,if this exception is about IDENTITY,please connect to the dba "
                            + "to make sure to statement the IDENTITY whit \"GENERATED BY DEFAULT AS IDENTITY "
                            + "(START WITH 0, INCREMENT BY 1, NO CACHE)\" rather than with \"GENERATED ALWAYS "
                            + "AS IDENTITY (START WITH 0, INCREMENT BY 1, NO CACHE)\" on table {}\n{},\n{}",
                    record.getTableName(), e.getMessage());
            throw new Exception(e);
        } finally {
            // enableFkConstraint(record.getTableName());
        }
    }

    @Override
    protected boolean quotedColumnName() {
        return false;
    }

    @Override
    protected boolean quotedTableName() {
        return false;
    }

    @Override
    protected String fkContraintNameColumn() {
        return "constraint_name";
    }

    @Override
    protected String buildQueryFKConstraintSql(String tableName) {
        return String.format(QUERY_FK_CONSTRAINT_SQL, getSchema(),
                tableName.toUpperCase());
    }

    @Override
    protected String getDisableFkConstraintSql() {
        return DISABLE_FK_CONSTRAINT_SQL;
    }

    @Override
    protected String getEnableFkConstraintSql() {
        return ENABLE_FK_CONSTRAINT_SQL;
    }

}
