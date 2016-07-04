package teclan.activejdbc.service.spi;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.service.DbType;

public class DamengService extends OracleService {

    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";
    private static final String URL_TEMPLATE = "jdbc:dm://%s:%d/%s";

    public DamengService(String name, String type, String host, int port,
            String dbName, String schema, String user, String password) {
        super(name, type, host, port, dbName, schema, user, password);
    }

    public DamengService(String name, DataSource dataSource) {
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
        return DbType.DAMENG;
    }

}
