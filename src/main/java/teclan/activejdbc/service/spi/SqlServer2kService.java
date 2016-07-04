package teclan.activejdbc.service.spi;

import java.util.List;

import org.javalite.activejdbc.DB;

import com.google.common.base.Joiner;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.model.DbRecord;
import teclan.activejdbc.service.DbType;
import teclan.activejdbc.service.DefaultDbService;

public class SqlServer2kService extends DefaultDbService {

    private static final String DRIVER_CLASS = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
    private static final String URL_TEMPLATE = "jdbc:microsoft:sqlserver://%s:%d;DatabaseName=%s";

    private static final String SELECT_PK_NAMES_SQL_TEMPLATE = "SELECT c.name AS name "
            + "FROM sysobjects o INNER JOIN "
            + "sysindexes x ON o.id = x.id INNER JOIN "
            + "syscolumns c ON o.id = c.id INNER JOIN "
            + "sysindexkeys xk ON o.id = xk.id AND x.indid = xk.indid AND c.colid = xk.colid AND "
            + "x.keycnt >= xk.keyno "
            + "WHERE o.name = '%s' AND (o.type IN ('U')) AND (x.status & 32 = 0) "
            + "AND (CONVERT(bit, (x.status & 0x800) / 0x800) = 1)";

    private static final String SELECT_COLUMNS_SQL_TEMPLATE = "select o.name as table_name, c.name as column_name, t.name as data_type "
            + "from syscolumns c, sysobjects o, systypes t "
            + "where o.name = '%s' and c.id = o.id and c.xtype = t.xtype";

    private static final String QUERY_TABLES_SQL = "select table_name from information_schema.tables where table_type='base table' and table_schema='%s'";

    private static final String SELECT_TABLE_SQL_TEMPLATE               = "SELECT name FROM dbo.sysobjects WHERE name='%s'";
    private static final String CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE = "CREATE TABLE %s.%s "
            + "(id INTEGER IDENTITY(1,1) NOT NULL PRIMARY KEY,"
            + " dbName     VARCHAR(30)," + " tableName  VARCHAR(30),"
            + " pkNames    VARCHAR(2000)," + " newPkValues   VARCHAR(2000),"
            + " oldPkValues   VARCHAR(2000)," + " action     VARCHAR(20))";

    private static final String CREATE_TRIGGER_DELETE_SQL_TEMPLATE = "create trigger %s.%s on %s.%s "
            + "after %s as declare @i int set @i=(select count(table_name) from information_schema.tables"
            + " where table_type='base table' and table_schema='%s' and table_name='%s') "
            + "if @i=1 drop table %s ;select * into %s from deleted; declare @rows int %s "
            + "set @rows=(select count(*) from %s) while @rows>0 begin "
            + "%s insert into %s.%s (dbName,tableName,pkNames,newPkValues,oldPkValues,action) "
            + "values ('%s','%s','%s',%s,%s,'%s'); delete from %s where %s set @rows=(select count(*) from %s) end;drop table %s";

    private static final String SELECT_TRIGGER_SQL_TEMPLATE = "SELECT name FROM dbo.sysobjects WHERE type='TR' AND name='%s'";
    private static final String CREATE_TRIGGER_SQL_TEMPLATE = "CREATE TRIGGER %s.%s ON %s.%s "
            + "FOR %s AS DECLARE %s BEGIN " + "%s INSERT INTO %s.%s"
            + "(dbName, tableName, pkNames, newPkValues, oldPkValues, action)"
            + "VALUES ('%s', '%s', '%s', %s, %s , '%s') END";

    private static final String QUERY_FK_CONSTRAINT_SQL = "select distinct a.table_name, b.column_name,b.constraint_name,object_name(rkeyid) r_table_name "
            + "from information_schema.table_constraints a, information_schema.constraint_column_usage b, sysforeignkeys c "
            + " where a.constraint_type='foreign key' and a.constraint_name= b.constraint_name and object_name(fkeyid)=a.table_name "
            + "and object_name(constid)=a.constraint_name and a.table_name='%s'";

    private static final String DISABLE_FK_CONSTRAINT_SQL = "alter table %s nocheck constraint %s";
    private static final String ENABLE_FK_CONSTRAINT_SQL  = "alter table %s check constraint %s";

    private List<String> columns;

    public SqlServer2kService(String name, String type, String host, int port,
            String dbName, String schema, String user, String password) {
        super(name, type, host, port, dbName, schema, user, password);
    }

    public SqlServer2kService(String name, DataSource dataSource) {
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
        return DbType.SQLSERVER2K;
    }

    @Override
    protected String getSelectPkNamesSql(final String table) {
        return String.format(SELECT_PK_NAMES_SQL_TEMPLATE, table, table);
    }

    @Override
    protected String getPkColumnName() {
        return "name";
    }

    @Override
    protected String getSelectColumnsSql(String tableName) {
        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, tableName);
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
    public boolean openDatabase() {
        try {

            new DB(getName()).open(getDriverClass(), getUrl(), getUsername(),
                    getPassword());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    @Override
    public void closeDatabase() {
        try {
            new DB(getName()).close();
        } catch (Exception e) {

        }
    }

    @Override
    public boolean openDatabase(String name) {
        try {

            new DB(name).open(getDriverClass(), getUrl(), getUsername(),
                    getPassword());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    @Override
    public void closeDatabase(String name) {
        new DB(name).close();
    }

    @Override
    protected String getCreateEventTableSql() {
        return String.format(CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE,
                getSchema(), getEventTableName());
    }

    @Override
    protected String getSelectTriggerSql(String tableName, String action) {
        return String.format(SELECT_TRIGGER_SQL_TEMPLATE,
                getTriggerName(tableName, action));
    }

    @Override
    protected String getCreateTriggerSql(String tableName, String action) {
        String declare = "";
        String values = "";
        String newPkValuesString = "";
        String oldPkValuesString = "";

        List<String> pkNames = getPkNames(tableName);

        if (pkNames.isEmpty()) {
            return null;
        }

        String schema = getSchema();
        String pkNamesString = Joiner.on(",").join(pkNames);

        if ("DELETE".equals(action.toUpperCase())) {
            String setSql = "";
            String condition = "";
            String tempTableForDeleted = getTempTableForDeleted(tableName);
            for (String pkName : pkNames) {
                declare += String.format(" declare @%s varchar(500)", pkName);
                setSql += String.format(" set @%s=(select top 1 %s from %s) ",
                        pkName, pkName, tempTableForDeleted);

                oldPkValuesString += String.format("@%s+','+", pkName);

                condition += String.format(" %s=@%s and ", pkName, pkName);
            }

            oldPkValuesString = oldPkValuesString.substring(0,
                    oldPkValuesString.lastIndexOf("+','+"));
            condition = condition.substring(0, condition.lastIndexOf("and"));

            return String.format(CREATE_TRIGGER_DELETE_SQL_TEMPLATE, schema,
                    getTriggerName(tableName, action), schema, tableName,
                    action, schema, tempTableForDeleted, tempTableForDeleted,
                    tempTableForDeleted, declare, tempTableForDeleted, setSql,
                    schema, getEventTableName(), getDbName(), tableName,
                    pkNamesString, "'null'", oldPkValuesString, action,
                    tempTableForDeleted, condition, tempTableForDeleted,
                    tempTableForDeleted);
        }

        for (String pkName : pkNames) {
            // for new pk values
            newPkValuesString += "@" + "new" + pkName + "+','+";
            declare += "@" + "new" + pkName + " VARCHAR(20),";
            values += "select @" + "new" + pkName + "=CONVERT(VARCHAR(200),"
                    + pkName + ")" + " from inserted ";

            // for old pk values
            oldPkValuesString += "@" + "old" + pkName + "+','+";
            declare += "@" + "old" + pkName + " VARCHAR(20),";
            values += "select @" + "old" + pkName + "=CONVERT(VARCHAR(200),"
                    + pkName + ")" + " from deleted ";
        }

        declare = declare.substring(0, declare.lastIndexOf(","));
        newPkValuesString = newPkValuesString.substring(0,
                newPkValuesString.lastIndexOf("+','+"));
        oldPkValuesString = oldPkValuesString.substring(0,
                oldPkValuesString.lastIndexOf("+','+"));

        return String.format(CREATE_TRIGGER_SQL_TEMPLATE, schema,
                getTriggerName(tableName, action), schema, tableName, action,
                declare, values, schema, getEventTableName(), getDbName(),
                tableName, pkNamesString, newPkValuesString, oldPkValuesString,
                action);
    }

    private String getTempTableForDeleted(String tableName) {
        return String.format("%s_delete_tmp", tableName);
    }

    @Override
    protected String getSelectTable(String tableName) {
        return String.format(SELECT_TABLE_SQL_TEMPLATE, tableName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handle(DbRecord record) throws Exception {

        try {
            String tableName = record.getTableName();
            // disableFkConstraint(record.getTableName());

            String action = record.getAction().toLowerCase();
            if (action.equals("insert")) {

                try {
                    new DB(getName())
                            .exec(getIdentityInsertSql(tableName, "ON"));
                } catch (Exception e) {
                    LOGGER.error("尝试关闭自增约束失败,表 {} 不存在自增主键,请忽略此信息\n", tableName);
                }
                // 此处不仅是为了获取params,而且还获取实际的非空字段
                Object[] params = generateParams(record.getPkFields(),
                        record.getDbFields());
                new DB(getName()).exec(generateInsertSql(record, columns),
                        params);

                try {
                    new DB(getName())
                            .exec(getIdentityInsertSql(tableName, "OFF"));
                } catch (Exception e) {
                    LOGGER.error("尝试关闭自增约束失败,表 {} 不存在自增主键,请忽略此信息\n", tableName);
                }

            } else if (action.equals("update")) {

                try {
                    new DB(getName())
                            .exec(getIdentityInsertSql(tableName, "ON"));
                } catch (Exception e) {
                    LOGGER.error("尝试关闭自增约束失败,表 {} 不存在自增主键,请忽略此信息\n", tableName);
                }

                try {
                    new DB(getName()).exec(generateUpdateSql(record),
                            generateParams(record.getDbFields(),
                                    record.getPkFields()));
                } catch (Exception e) {
                    // 当表中含有自增主键时更新数据会导致异常(因为每次更新数据是更新所有字段的数据,
                    // 并不只是被更改的数据而已,然而自增主键是不允许做修改的),故先将原记录删除再重新插入
                    try {
                        new DB(getName()).exec(generateDeleteSql(record),
                                generateParams(record.getPkFields()));
                        Object[] params = generateParams(record.getDbFields());
                        new DB(getName()).exec(
                                generateInsertSql(record, columns), params);
                    } catch (Exception e1) {
                        LOGGER.error(e1.getMessage(), e1);
                    }
                }

                try {
                    new DB(getName())
                            .exec(getIdentityInsertSql(tableName, "OFF"));
                } catch (Exception e) {
                    LOGGER.error("尝试关闭自增约束失败,表 {} 不存在自增主键,请忽略此信息\n", tableName);
                }

            } else if (action.equals("delete")) {
                new DB(getName()).exec(generateDeleteSql(record),
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

    private String getIdentityInsertSql(String tableName, String swtch) {
        return String.format("SET IDENTITY_INSERT [%s].[%s] %s", getSchema(),
                tableName, swtch.toUpperCase());
    }

    @Override
    protected String fkContraintNameColumn() {
        return "constraint_name";
    }

    @Override
    protected String buildQueryFKConstraintSql(String tableName) {
        return String.format(QUERY_FK_CONSTRAINT_SQL, tableName);
    }

    @Override
    protected String getDisableFkConstraintSql() {
        return DISABLE_FK_CONSTRAINT_SQL;
    }

    @Override
    protected String getEnableFkConstraintSql() {
        return ENABLE_FK_CONSTRAINT_SQL;
    }

    protected String generateInsertSql(DbRecord dbRecord,
            List<String> columns) {

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                getTableNameWithSchema(dbRecord), Joiner.on(",").join(columns),
                generatePlaceHolder(columns.size()));
    }

}
