package teclan.activejdbc.service.spi;

import java.util.List;

import org.javalite.activejdbc.DB;

import com.google.common.base.Joiner;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.service.DbType;
import teclan.activejdbc.service.DefaultDbService;

public class KingbaseService extends DefaultDbService {

    private static final String URL_TEMPLATE = "jdbc:kingbase://%s:%d/%s";
    private static final String DRIVER_CLASS = "com.kingbase.Driver";

    private static final String SELECT_COLUMNS_SQL_TEMPLATE = "select column_name, data_type "
            + "from information_schema.columns "
            + "where table_schema = '%s' and table_name = '%s'";

    private static final String QUERY_TABLES_SQL = "SELECT table_name FROM information_schema.tables WHERE table_schema='%s'";

    private static final String SELECT_TABLE_SQL_TEMPLATE = "SELECT table_name FROM information_schema.tables "
            + "WHERE table_schema='%s' AND table_name='%s'";

    private static final String CREATE_SEQUENCE_SQL_TEMPLATE_FOR_TRIGGER_EVENTS = "CREATE SEQUENCE %s.%s START 1 INCREMENT 1";

    private static final String CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE = "CREATE TABLE %s.%s "
            + "( id INT DEFAULT NEXTVAL('%s.%s'), " + "dbName VARCHAR(20), "
            + "tableName VARCHAR(20), pkNames VARCHAR(255), "
            + "newPkValues VARCHAR(255), oldPkValues VARCHAR(255),"
            + "action VARCHAR(20), PRIMARY KEY(id))";

    private static final String DROP_TRIGGER_EVENT_TABLE_SQL_TEMPLATE = "DROP TABLE %s.%s;"
            + "DROP SEQUENCE %s.%s";

    private static final String SELECT_TRIGGER_SQL_TEMPLATE = "SELECT trigger_name FROM information_schema.triggers "
            + "WHERE trigger_schema='%s' AND trigger_name='%s' "
            + "AND event_manipulation='%s'";

    private static final String CREATE_OR_REPLACE_TRIGGER_SQL_TEMPLATE = "CREATE OR REPLACE TRIGGER %s %s %s ON %s.\"%s\" "
            + "FOR EACH ROW AS BEGIN INSERT INTO %s.%s "
            + "(dbName, tableName, pkNames, newPkValues, oldPkValues,action) "
            + "VALUES ('%s','%s','%s',(%s),(%s),'%s'); END;";
    private static final String SELECT_PK_NAMES_SQL_TEMPLATE           = "SELECT a.column_name FROM information_schema.key_column_usage a "
            + "INNER JOIN information_schema.table_constraints b "
            + "ON (a.constraint_name = b.constraint_name) "
            + "WHERE a.table_schema = '%s' AND b.table_schema = '%s' "
            + "AND b.table_catalog = '%s' AND b.table_name = '%s' "
            + "AND b.constraint_type = 'PRIMARY KEY'";

    private static final String QUERY_FK_CONSTRAINT_SQL   = "select sys_class.relname as table_name,sys_attribute.attname as column "
            + "name,fk.conname as constraint_name form (select unnest(sys_constraint.conkey),conname,sys_constraint.conrelid,"
            + "sys_constraint.confrelid form sys_constraint,sys_namespace where sys_constraint.connamespace=sys_namespace.oid "
            + "and sys_namespace.nspname=’%s’ and contype=’f’ )fk,sys_attribute,sys_class,all_constraints where fk.unnest=sys_attribute.attnum "
            + "and fk.conrelid=sys_attribute.attrelid and fk.confrelid=sys_class.oid and fk.conname=all_constraints.constaint_name and sys_class.relname='%s'";
    private static final String DISABLE_FK_CONSTRAINT_SQL = "alter table %S alter foreign key %s not enforced";
    private static final String ENABLE_FK_CONSTRAINT_SQL  = "alter table %s alter foreign key %s enforced";

    public KingbaseService(String name, String type, String host, int port,
            String dbName, String schema, String user, String password) {
        super(name, type, host, port, dbName, schema, user, password);
    }

    public KingbaseService(String name, DataSource dataSource) {

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
        return DbType.KINGBASE;
    }

    @Override
    protected String getSelectPkNamesSql(final String table) {
        return String.format(SELECT_PK_NAMES_SQL_TEMPLATE, getSchema(), table);
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
        new DB(getName()).close();
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
        String schema = getSchema().toUpperCase();

        String createSeqSql = String.format(
                CREATE_SEQUENCE_SQL_TEMPLATE_FOR_TRIGGER_EVENTS, getSchema(),
                getEventTableIdSeq());

        try {
            new DB(getName()).exec(createSeqSql);
        } catch (Exception e) {
            LOGGER.error(
                    " When you see this message mean the sequence "
                            + getEventTableIdSeq() + " in shcema " + schema
                            + " is aready exist,please ignore !!!",
                    e.getMessage());
        }

        return String.format(CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE, schema,
                getEventTableName(), schema, getEventTableIdSeq());
    }

    @Override
    protected String getDropEventTableSql() {
        String schema = getSchema().toUpperCase();
        return String.format(DROP_TRIGGER_EVENT_TABLE_SQL_TEMPLATE, schema,
                getEventTableName(), schema, getEventTableIdSeq());
    }

    @Override
    protected String getSelectTriggerSql(String tableName, String action) {
        return String.format(SELECT_TRIGGER_SQL_TEMPLATE,
                getSchema().toUpperCase(), getTriggerName(tableName, action),
                action.toUpperCase());
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

        String schema = getSchema().toUpperCase();

        if ("UPDATE".equalsIgnoreCase(action)) {
            newPkValues = getPKContentsExpression(pkNames, "NEW");
            oldPkValues = getPKContentsExpression(pkNames, "OLD");

        } else if ("DELETE".equalsIgnoreCase(action)) {
            oldPkValues = getPKContentsExpression(pkNames, "OLD");

        } else if ("INSERT".equals(action)) {
            newPkValues = getPKContentsExpression(pkNames, "NEW");
        }

        return String.format(CREATE_OR_REPLACE_TRIGGER_SQL_TEMPLATE,
                getTriggerName(tableName, action), "AFTER", action, schema,
                tableName, schema, getEventTableName(), getDbName(), tableName,
                pkNamesString, newPkValues, oldPkValues, action);
    }

    private String getPKContentsExpression(List<String> pkNames,
            String prefix) {
        String[] prefixedPkNames = new String[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            prefixedPkNames[i] = String.format("%s.\"%s\"", prefix,
                    pkNames.get(i));
        }

        return Joiner.on("||','||").join(prefixedPkNames);
    }

    @Override
    protected String getSelectTable(String tableName) {
        return String.format(SELECT_TABLE_SQL_TEMPLATE, getSchema(), tableName);
    }

    @Override
    protected String fkContraintNameColumn() {
        return "constraint_name";
    }

    @Override
    protected String buildQueryFKConstraintSql(String tableName) {
        return String.format(QUERY_FK_CONSTRAINT_SQL, getSchema(), tableName);
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
