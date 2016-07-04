package teclan.activejdbc.service;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.service.DbService;
import teclan.activejdbc.service.DbType;
import teclan.activejdbc.service.spi.ServiceFactory;
import teclean.activejdbc.model.StudentTest;

public class FactoryTest {
    final Logger LOGGER = LoggerFactory.getLogger(StudentTest.class);

    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String URL_TEMPLATE = "jdbc:mysql://%s:%d/%s";

    private static final String TYPE     = "mysql";
    private static final String HOST     = "127.0.0.1";
    private static final int    PORT     = 3306;
    private static final String DB_NAME  = "testdb";
    private static final String SCHEMA   = "testdb";
    private static final String USER     = "root";
    private static final String PASSWORD = "123456";

    DataSource dataSource = null;

    DbService service;

    @Test
    public void create() {

        dataSource = new DataSource(TYPE, HOST, PORT, DB_NAME, SCHEMA, USER,
                PASSWORD, DRIVER_CLASS, URL_TEMPLATE);

        service = ServiceFactory.getService("default", dataSource);

        LOGGER.info(service.getDbType().getValue());

        assertTrue(DbType.MYSQL.equals(service.getDbType()));

    }

}
