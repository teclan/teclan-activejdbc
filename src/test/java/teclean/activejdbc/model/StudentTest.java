package teclean.activejdbc.model;

import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.db.Database;
import teclan.activejdbc.service.DbService;

public class StudentTest {
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

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    DataSource       dataSource = null;
    Database         database   = null;

    private DbService service;

    @Before
    public void setUp() {
        dataSource = new DataSource(TYPE, HOST, PORT, DB_NAME, SCHEMA, USER,
                PASSWORD, DRIVER_CLASS, URL_TEMPLATE);

        database = new Database(dataSource);

        try {
            // database.setMigrate(true);
            // database.setMigrateClean(true);
            database.initDb();
            // database.setMigrateClean(false);
            // database.setMigrate(false);

            database.openDatabase();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @After
    public void setDown() {
        database.closeDatabase();
    }

    @Test
    public void validateTableName() {
        assertTrue(new Student().getConfigTableName()
                .equals(Student.getTableName()));
    }

    @Test
    public void DataTest() {
        Student p = new Student();
        p.set("name", "Declan");
        p.set("enty_time", dateFormat.format(new Date()));
        p.set("age", 100);
        p.set("sex", "man");
        p.saveIt();

        assertTrue(1 == Student.count());

        Student student = Student.findFirst("name = ?", "Declan");
        LOGGER.info(student.toJson());
        assertTrue("Declan".equals(student.getString("name")));

        Student.deleteAll();

        assertTrue(0 == Student.count());
    }

    @Test
    public void scriptFileTest() {

    }

}
