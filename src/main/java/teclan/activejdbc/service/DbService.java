package teclan.activejdbc.service;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface DbService {

    public String getDriverClass();

    public String getUrlTemplate();

    public ArrayList<String> getTables() throws SQLException;

    public boolean hasTable(String tableName);

    public boolean openDatabase();

    public void closeDatabase();

    public boolean openDatabase(String name);

    public void closeDatabase(String name);

    public boolean hasConnected();

    public boolean hasConnected(String name);

    public boolean Triggerexists(String tableName, String action);

    public void createTrigger(String tableName);

    public void removeTrigger(String tableName);

    public void createEventTable();

    public void dropEventTable();

    public DbType getDbType();

    public void retrieve(String table);

    public void retrieveWithoutAdapter(String table, String action,
            String[] pkNames, Object[] newPkValues, Object[] oldPkValues);

    public void retrieve(String table, String action, String[] pkNames,
            Object[] newPkValues, Object[] oldPkValues);

    public Map<String, String> getTableColumns(String tableName);

    public List<String> getPkNames(String table);

    public void execute(String sql);

    public void execute(String name, String sql);

    public void execute(File file);

    public void execute(String name, File file);

}
