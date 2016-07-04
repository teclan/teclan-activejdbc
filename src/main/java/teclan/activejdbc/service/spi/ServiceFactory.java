package teclan.activejdbc.service.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.service.DbService;

public class ServiceFactory {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ServiceFactory.class);

    private static DbService dbService = null;

    private static DataSource dataSource = null;

    public static DbService getService(String name, String type, String host,
            int port, String dbName, String schema, String user,
            String password) {

        dataSource = new DataSource(type, host, port, dbName, schema, user,
                password);

        return getService(name, dataSource);
    }

    public static DbService getService(String name, DataSource dataSource) {

        String type = dataSource.getType();

        if ("ORACLE".equals(type.toUpperCase())) {

            return new OracleService(name, dataSource);

        }

        if ("DAMENG".equals(type.toUpperCase())) {

            return new DamengService(name, dataSource);

        }

        if ("SQLSERVER2K".equals(type.toUpperCase())) {

            return new SqlServer2kService(name, dataSource);

        }

        if ("SQLSERVER2KPLUS".equals(type.toUpperCase())) {

            return new Sqlserver2kPlusService(name, dataSource);

        }

        if ("MYSQL".equals(type.toUpperCase())) {

            return new MysqlService(name, dataSource);

        }

        if ("DB2".equals(type.toUpperCase())) {

            return new Db2Service(name, dataSource);

        }

        // Add more db

        if (dbService == null) {
            LOGGER.error(
                    "无法匹配数据库类型,支持的数据库类型包括:ORACLE,DAMENG,SQLSERVER2K,SQLSERVER2KPLUS,MYSQL,DB2;   type:{}",
                    type);
        }

        return null;

    }

}
