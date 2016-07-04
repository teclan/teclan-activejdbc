package teclan.activejdbc.service;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.text.SimpleDateFormat;

import org.javalite.activejdbc.LazyList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.db.Database;
import teclan.activejdbc.service.RetrieverListener;
import teclan.activejdbc.service.spi.MysqlService;
import teclean.activejdbc.model.Student;
import teclean.activejdbc.model.StudentTest;
import teclean.activejdbc.model.TriggerEvent;

public class MysqlServiceTest {
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

    private static final String RESOURCE_ROOT_PATH = System
            .getProperty("user.dir") + File.separator + "src" + File.separator
            + "test" + File.separator + "resources" + File.separator + "%s"
            + File.separator + "sql";

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    DataSource       dataSource = null;
    Database         database   = null;

    MysqlService mysqlService = null;

    RetrieverListener retrieverListener = null;

    @Before
    public void setUp() {
        dataSource = new DataSource(TYPE, HOST, PORT, DB_NAME, SCHEMA, USER,
                PASSWORD, DRIVER_CLASS, URL_TEMPLATE);

        mysqlService = new MysqlService("default", dataSource);

        try {

            mysqlService.openDatabase();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

    }

    @After
    public void setDown() {
        mysqlService.closeDatabase();
    }

    @Test
    public void sqlWithMultiFile() {

        String path = getFilePath("create_student_table.sql");

        assertFalse(mysqlService.hasTable("stuent"));

        mysqlService.execute(new File(path));

        assertTrue(mysqlService.hasTable("student"));

        if (mysqlService.hasTable("student")) {
            path = getFilePath("drop_student_table.sql");
            mysqlService.execute(new File(path));

        }

        assertFalse(mysqlService.hasTable("student"));

    }

    @Test
    public void sqlWithOneFile() {

        String path = getFilePath("student.sql");

        assertFalse(mysqlService.hasTable("stuent"));

        mysqlService.execute(new File(path));

        assertTrue(mysqlService.hasTable("student"));

        LazyList<Student> students = Student.findAll();

        for (Student stu : students) {
            LOGGER.info(stu.toString());
        }

        mysqlService.execute("drop table testdb.student");

        assertFalse(mysqlService.hasTable("student"));

    }

    @SuppressWarnings("unused")

    @Test
    public void retriverTest() {

        retrieverListener = new MysqlRetriverListener();

        mysqlService.setRetrieverListener(retrieverListener);

        String path = getFilePath("student.sql");

        String tableName = "student";
        mysqlService.execute(new File(path));

        assertTrue(mysqlService.hasTable(tableName));

        LazyList<Student> students = Student.findAll();

        for (Student stu : students) {
            LOGGER.info(stu.toString());
        }

        LOGGER.info("retriver:");

        mysqlService.retrieve(tableName);

        mysqlService.execute("drop table testdb.student");

    }

    @Test
    public void handleTest() {

        retrieverListener = new MysqlRetriverListener();

        mysqlService.setRetrieverListener(retrieverListener);

        String tableName = "student";
        String path = getFilePath("create_student_table.sql");

        // mysqlService.execute("drop table student");

        assertFalse(mysqlService.hasTable(tableName));

        mysqlService.execute(new File(path));

        assertTrue(mysqlService.hasTable(tableName));
        assertTrue(0 == Student.count());

        path = getFilePath("insert_student_table.sql");

        mysqlService.execute(new File(path));
        assertTrue(1 == Student.count());

        LOGGER.info("retriver:");
        mysqlService.retrieve(tableName);

        path = getFilePath("delete_student_table.sql");
        mysqlService.execute(new File(path));
        assertTrue(0 == Student.count());

        // 至此,student 表中已经没有数据了,接下来使用刚从取回的 DbRecord 写回数据库
        try {
            mysqlService.handle(retrieverListener.getDbRecord());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

        assertTrue(1 == Student.count());

        showStudentTable();

        mysqlService.execute("drop table testdb.student");

    }

    private String getFilePath(String fileName) {

        return String.format(RESOURCE_ROOT_PATH, dataSource.getType())
                + File.separator + fileName;
    }

    private void showRecords() {

        if (mysqlService.hasTable("student")) {
            showStudentTable();
        }

        if (mysqlService.hasTable(TriggerEvent.getTableName())) {
            showEventsTable();
        }

    }

    private void showStudentTable() {

        LazyList<Student> students = Student.findAll();

        LOGGER.info("=================== {} ==================",
                Student.getTableName());
        for (Student stu : students) {
            LOGGER.info(stu.toString());
        }

    }

    private void showEventsTable() {

        LazyList<TriggerEvent> triggerEvents = TriggerEvent.findAll();

        LOGGER.info("=================== {} ==================",
                TriggerEvent.getTableName());
        for (TriggerEvent event : triggerEvents) {
            LOGGER.info(event.toString());
        }
    }

}
