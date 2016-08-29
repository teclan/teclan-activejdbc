package teclan.activejdbc.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import teclan.activejdbc.db.DataSource;
import teclan.activejdbc.service.DataType;
import teclan.utils.Bytes;

public class DbFieldFactory {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DbFieldFactory.class);

    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String ORACLE_URL_TEMPLATE = "jdbc:oracle:thin:@%s:%d:%s";

    public DbField granerate(String key, Object value, DataType dataType,
            DataSource dataSource) {

        Object object = value;
        if (DataType.DATETIME.equals(dataType)) {
            if (value instanceof java.util.Date) {
                object = ((java.util.Date) value).getTime();
            } else if (value instanceof java.sql.Time) {
                object = ((java.sql.Time) value).getTime();
            } else if (value instanceof java.sql.Timestamp) {
                object = ((java.sql.Timestamp) value).getTime();
            } else if (value instanceof oracle.sql.TIMESTAMP) {
                try {
                    object = ((oracle.sql.TIMESTAMP) value).timestampValue()
                            .getTime();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            } else if (value instanceof oracle.sql.TIMESTAMPLTZ) {
                object = granerateValuesForOracleWithTimeZone(value,
                        TIMESTAMP.TIMESTAMPTZ, dataSource);
            } else if (value instanceof oracle.sql.TIMESTAMPTZ) {
                object = granerateValuesForOracleWithTimeZone(value,
                        TIMESTAMP.TIMESTAMPLTZ, dataSource);
            }
        }
        // for BIT
        if (DataType.INTEGER.equals(dataType)) {
            if (value.equals(true)) {
                object = 1;
            } else if (value.equals(false)) {
                object = 0;
            }
        }
        // for mysql，sqlserver2k,sqlserver2k+ BINARY
        if (DataType.COMMON_BINARY.equals(dataType)) {

            object = "0x" + Bytes.toHexString((byte[]) value);
            dataType = DataType.STRING;
        }
        // for kingbase
        if (DataType.BOOLEAN.equals(dataType)) {
            if (value.equals("1")) {
                object = true;
            } else {
                object = false;
            }
        }
        return new DbField(key, object, dataType);
    }

    /**
     * 特殊处理 oracle 字段 TIMESTAMP(6) WITH LOCAL TIME ZONE，TIMESTAMP(6) WITH TIME
     * ZONE
     * 
     * @param key
     * @param timstamp
     * @param dataSource
     * @return
     */
    public static long granerateValuesForOracleWithTimeZone(Object value,
            TIMESTAMP timstamp, DataSource dataSource) {

        Connection conn = null;

        long time = 0;

        try {
            Class.forName(ORACLE_DRIVER_CLASS);

            // 尝试使用 conn = new DB(name).getConnection() 或 conn =new
            // DB(name).connection()时，在调用 toTIMESTAMP(Connection conn,bye[]
            // byets)时报异常
            // com.zaxxer.hikari.proxy.HikariConnectionProxy cannot be cast to
            // oracle.jdbc.OracleConnection
            conn = DriverManager.getConnection(
                    String.format(ORACLE_URL_TEMPLATE, dataSource.getHost(),
                            dataSource.getPort(), dataSource.getDbName()),
                    dataSource.getUser(), dataSource.getPassword());

            if (conn == null) {
                LOGGER.warn(
                        "open database faild when retriver the value of column with \"TIMESTAMP(6) WITH TIME ZONE\" "
                                + "or \"TIMESTAMP(6) WITH LOCAL TIME ZONE\",and we will return the time of this exception happen instead!");

                return new Date().getTime();
            }

            if (TIMESTAMP.TIMESTAMPTZ.equals(timstamp)) {
                time = TIMESTAMPTZ
                        .toTIMESTAMP(conn,
                                ((oracle.sql.TIMESTAMPTZ) value).toBytes())
                        .timestampValue().getTime();
            } else {
                time = TIMESTAMPLTZ
                        .toTIMESTAMP(conn,
                                ((oracle.sql.TIMESTAMPLTZ) value).toBytes())
                        .timestampValue().getTime();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
            }
        }

        return time;
    }

    public enum TIMESTAMP {
        TIMESTAMPTZ("TIMESTAMPTZ"), TIMESTAMPLTZ("TIMESTAMPLTZ");
        public String value;

        private TIMESTAMP(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
