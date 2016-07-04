package teclan.activejdbc.service.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.javalite.activejdbc.DB;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.service.DbType;
import teclan.activejdbc.service.DefaultDbService;

public class OracleService extends DefaultDbService {

    private static final String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String URL_TEMPLATE = "jdbc:oracle:thin:@%s:%d:%s";

    private static final String SELECT_PK_NAMES_SQL_TEMPLATE                    = "SELECT column_name FROM user_cons_columns "
            + "WHERE constraint_name = ( "
            + "SELECT constraint_name FROM user_constraints "
            + "WHERE table_name= '%s' AND constraint_type ='P')";
    private static final String SELECT_COLUMNS_SQL_TEMPLATE                     = "select column_name, data_type "
            + "from all_tab_columns "
            + "where owner = '%s' and table_name = '%s'";
    private static final String QUERY_TABLES_SQL                                = "select table_name from all_tables where owner=upper('%s')";
    private static final String SELECT_TABLE_SQL_TEMPLATE                       = "SELECT 1 FROM user_tables WHERE table_name='%s'";
    private static final String SELECT_SEQUENCE_SQL_TEMPLATE                    = "select SEQUENCE_NAME from user_sequences where SEQUENCE_NAME = '%s'";
    private static final String CREATE_SEQUENCE_SQL_TEMPLATE_FOR_TRIGGER_EVENTS = "CREATE SEQUENCE \"%s\".\"%s\" "
            + "INCREMENT BY 1 START WITH 0 MINVALUE 0 NOMAXVALUE";
    private static final String CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE         = "CREATE TABLE %s.%s (  "
            + "  id              NUMBER          NOT NULL PRIMARY KEY, "
            + "  dbName          VARCHAR2(50)    NOT NULL, "
            + "  tableName       VARCHAR2(50)    NOT NULL, "
            + "  pkNames         VARCHAR2(2000)  NOT NULL, "
            + "  newPkValues     VARCHAR2(2000),          "
            + "  oldPkValues     VARCHAR2(2000),           "
            + "  action          VARCHAR2(20)    NOT NULL " + " )";

    private static final String DROP_SEQUENCE_SQL_TEMPLATE_FOR_TRIGGER_EVENTS = "DROP SEQUENCE %s.%s";
    private static final String SELECT_TRIGGER_SQL_TEMPLATE                   = "SELECT trigger_name FROM all_triggers "
            + "WHERE owner='%s' AND table_name='%s' AND trigger_name='%s'";
    @SuppressWarnings("unused")
    private static final String SELECT_PK_NAMES_SQL_TEMPLATE1                 = "select cols.table_name, cols.column_name, cols.constraint_name from user_constraints cons, "
            + "user_cons_columns cols where cons.constraint_type = 'P' and cons.constraint_name = cols.constraint_name and cons.owner = cols.owner  and cols.table_name='%s' "
            + "and cols.owner='%s'  order by cols.table_name, cols.position";
    private static final String CREATE_TRIGGER_SQL_TEMPLATE                   = "CREATE OR REPLACE TRIGGER \"%s\".\"%s\" AFTER %s ON \"%s\".\"%s\" "
            + "FOR EACH ROW BEGIN INSERT INTO \"%s\".\"%s\" "
            + "VALUES (%s.nextval ,'%s','%s','%s',(%s),(%s),'%s');END;";

    private static final String QUERY_FK_CONSTRAINT_SQL   = "select ucc.constraint_name from user_cons_columns ucc "
            + "join user_constraints uc on ucc.owner=uc.owner and ucc.constraint_name= uc.constraint_name "
            + "join user_constraints uc_pk ON uc.r_owner= uc_pk.owner and uc.r_constraint_name=uc_pk.constraint_name "
            + "where uc.constraint_type='R' and ucc.table_name='%s'";
    private static final String DISABLE_FK_CONSTRAINT_SQL = "alter table %s.%s disable constraint %s";
    private static final String ENABLE_FK_CONSTRAINT_SQL  = "alter table %s.%s enable constraint %s";

    public OracleService(String name, String type, String host, int port,
            String dbName, String schema, String user, String password) {
        super(name, type, host, port, dbName, schema, user, password);
    }

    public OracleService(String name, DataSource dataSource) {

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
        return DbType.ORACLE;
    }

    @Override
    protected String tableNameColumn() {
        return "table_name";
    }

    @Override
    protected String getSelectPkNamesSql(final String table) {
        return String.format(SELECT_PK_NAMES_SQL_TEMPLATE, table);
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
        return String.format(QUERY_TABLES_SQL, getSchema().toUpperCase());
    }

    @Override
    protected String getSelectTable(String tableName) {
        return String.format(SELECT_TABLE_SQL_TEMPLATE, tableName);
    }

    @Override
    protected String getCreateEventTableSql() {

        return String.format(CREATE_TRIGGER_EVENT_TABLE_SQL_TEMPLATE,
                getSchema().toUpperCase(), getEventTableName());
    }

    @Override
    protected String getSelectTriggerSql(String tableName, String action) {
        return String.format(SELECT_TRIGGER_SQL_TEMPLATE,
                getSchema().toUpperCase(), tableName.toUpperCase(),
                getTriggerName(tableName, action));
    }

    @Override
    protected String getCreateTriggerSql(String tableName, String action) {
        List<String> pkNames = getPkNames(tableName);

        if (pkNames.isEmpty()) {
            return null;
        }

        String pkNamesString = String.join(",", pkNames);

        String newPkValues = getPKContentsExpression(pkNames, ":NEW");
        String oldPkValues = getPKContentsExpression(pkNames, ":OLD");

        String schema = getSchema();
        return String.format(CREATE_TRIGGER_SQL_TEMPLATE, schema,
                getTriggerName(tableName, action), action, schema, tableName,
                schema, getEventTableName(), getEventTableIdSeq(), getDbName(),
                tableName, pkNamesString, newPkValues, oldPkValues, action);
    }

    private String getPKContentsExpression(List<String> pkNames,
            String prefix) {
        String[] prefixedPkNames = new String[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            prefixedPkNames[i] = String.format("%s.\"%s\"", prefix,
                    pkNames.get(i));
        }

        return String.join("||','||", prefixedPkNames);
    }

    @SuppressWarnings("rawtypes")
    private boolean hasSequence() {

        List<Map> result = new DB(getName()).findAll(String
                .format(SELECT_SEQUENCE_SQL_TEMPLATE, getEventTableIdSeq()));

        return !result.isEmpty();
    }

    @Override
    protected String getTriggerName(String tableName, String action) {
        return String.format("DECLAN_%s_%s", tableName, action);
    }

    @Override
    public void createSequence() {
        if (!hasSequence()) {
            new DB(getName()).exec(String.format(
                    CREATE_SEQUENCE_SQL_TEMPLATE_FOR_TRIGGER_EVENTS,
                    getSchema(), getEventTableIdSeq()));
        }
    }

    @Override
    public void dropEventTable() {
        super.dropEventTable();

        String dropSeqSql = String.format(
                DROP_SEQUENCE_SQL_TEMPLATE_FOR_TRIGGER_EVENTS, getSchema(),
                getEventTableIdSeq());
        new DB(getName()).exec(dropSeqSql);
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
    public void disableFkConstraint(String tableName) {
        ArrayList<String> fk = getFKConstraint(tableName);

        if (fk.isEmpty()) {
            return;
        }

        for (String key : fk) {
            new DB(getName()).exec(String.format(getDisableFkConstraintSql(),
                    getSchema(), tableName, key));
        }
    }

    @Override
    public void enableFkConstraint(String tableName) {

        ArrayList<String> fk = getFKConstraint(tableName);

        if (fk.isEmpty()) {
            return;
        }

        for (String key : fk) {
            new DB(getName()).exec(String.format(getEnableFkConstraintSql(),
                    getSchema(), tableName, key));
        }
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
