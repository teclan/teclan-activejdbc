package teclan.activejdbc.db;

public class DataSource {

    private String type;
    private String host;
    private int    port;
    private String dbName;
    private String schema;
    private String user;
    private String password;
    private String driverClass;
    private String url;

    public DataSource() {

    }

    public DataSource(String type, String host, int port, String dbName,
            String schema, String user, String password) {
        this(type, host, port, dbName, schema, user, password, null, null);
    }

    public DataSource(String type, String host, int port, String dbName,
            String schema, String user, String password, String driverClass,
            String urlTemplate) {

        this.type = type;
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.schema = schema;
        this.user = user;
        this.password = password;
        this.driverClass = driverClass;
        this.url = urlTemplate == null ? null
                : String.format(urlTemplate, host, port, dbName);

    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUrlTemplate(String urlTemplate) {
        this.url = String.format(urlTemplate, host, port, dbName);
    }
}
