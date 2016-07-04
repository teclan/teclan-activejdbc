

 # 提供主流数据库（Oracle9i+,SqlServer2k+,mysql5.6+,DB28.5+,达梦7+，金仓）的常见操作支持
 
 ```Java

     String getDriverClass();

     String getUrlTemplate();

     ArrayList<String> getTables() throws SQLException;

     boolean hasTable(String tableName);

     boolean openDatabase();

     void closeDatabase();

     boolean openDatabase(String name);

     void closeDatabase(String name);

     boolean hasConnected();

     boolean hasConnected(String name);

     boolean Triggerexists(String tableName, String action);

     void createTrigger(String tableName);

     void removeTrigger(String tableName);

     void createEventTable();

     void dropEventTable();

     DbType getDbType();

     void retrieve(String table);

     void retrieveWithoutAdapter(String table, String action, String[] pkNames, Object[] newPkValues, Object[] oldPkValues);

     void retrieve(String table, String action, String[] pkNames,Object[] newPkValues, Object[] oldPkValues);

     Map<String, String> getTableColumns(String tableName);

     List<String> getPkNames(String table);

     void execute(String sql);

     void execute(String name, String sql);

     void execute(File file);

     void execute(String name, File file);

  ``` 

 关于Activejdbc,所有的表必须有 id 字段,并且是自增的,例如mysql数据库表结构应该如下:
 
 ```bash
 create table worker (
  id bigint not null AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(56) NOT NULL,
  ...
  ...
  entry_time DATETIME,
  created_at DATETIME,
  updated_at DATETIME
  )
  
  ```
  则应该有 
  ```Java
  
  @Table("worker")
  class Worker extends Model {  }
  ```
  
 如果表 worker 不存上述 id 字段,则在调用 Worker.create()或 Worker.createIt()
 或者 set()方法都不生效(save()之后也不报错)
 
 另外表中的 created_at 和 updated_at 是不允许手动设值的
 
 