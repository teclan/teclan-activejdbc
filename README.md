

  提供主流数据库（Oracle9i+,SqlServer2k+,mysql5.6+,DB28.5+,达梦7+，金仓）的常见操作支持，
 
  常见的数据库操作相关SQL，请点击[这里](https://teclan.github.io/2016/07/15/%E5%B8%B8%E8%A7%81%E6%95%B0%E6%8D%AE%E5%BA%93%E6%93%8D%E4%BD%9CSQL/)
 
# activeJdbc说明
  [activeJdbc在线文档](http://javalite.io/documentation)

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
 
# 项目接口介绍

[接口源码](https://github.com/teclan/teclan-activejdbc/blob/master/src/main/java/teclan/activejdbc/service/DbService.java)

## 获取Jdbc驱动
```Java
String getDriverClass();
```
## 获取数据库连接url
```Java
String getUrlTemplate();
``` 
## 打开数据库(默认连接“default”)
```Java
boolean openDatabase();
```
## 关闭数据库(默认连接“default”)
```Java
void closeDatabase();
```
## 打开数据库(指定连接)
```Java
boolean openDatabase(String name);
```
## 关闭数据库(指定连接)
```Java
void closeDatabase(String name);
```
## 获取所有表
```Java
ArrayList<String> getTables();
```
## 是否存在表
```Java
boolean hasTable(String tableName);
```
## 是否存在连接(默认连接“default”)
```Java
boolean hasConnected();
```
## 是否存在连接(指定连接)
```Java
boolean hasConnected(String name);
```
## 表上是否存在指定触发器
```Java
boolean Triggerexists(String tableName, String action);
```
## 创建触发器
关于创建触发器，项目中的触发器是用于记录表变化的(其他触发器请通过执行sql自行创建或这重写以下方法)，
变化记录存放在事件记录表中，事件记录表参考下方介绍。

```Java
void createTrigger(String tableName);
```
## 删除触发器
删除的是以上创建的触发器，删除其他触发器请通过执行sql自行创建或这重写以下方法。
```Java
public void removeTrigger(String tableName);
```
## 创建事件记录表
==========================================================
|id|dbName|tableName|pkNames|newPkVlues|oldPkValues|action|
==========================================================
```Java
void createEventTable();
```
## 删除事件记录表
```Java
public void dropEventTable();
```
## 获取数据库类型
```Java
DbType getDbType();
```
DbType定义如下：
```Java
public enum DbType {
    ORACLE("ORACLE"), 
    DAMENG("DAMENG"), 
    SQLSERVER2K("SQLSERVER2K"), 
    SQLSERVER2KPLUS("SQLSERVER2KPLUS"), 
    MYSQL("MYSQL"), 
    KINGBASE("KINGBASE"), 
    DB2("DB2");
    private String value;
    private DbType(String value) {
        this.value = value;
    }
    public String getValue() {
        return value;
    }
}
```
## 获取表数据
```Java
void retrieve(String table);
//适用于mysql，mysql数据库在一个查询结果集未处理完毕时，不允许在同一个查询中再次查询
void retrieveWithoutAdapter(String table, String action,String[] pkNames, Object[] newPkValues, Object[] oldPkValues);
//适用于除mysql外的数据库
void retrieve(String table, String action, String[] pkNames,Object[] newPkValues, Object[] oldPkValues);
```
获取到表的数据以后触发 [RetrieverListener](https://github.com/teclan/teclan-activejdbc/blob/master/src/main/java/teclan/activejdbc/service/RetrieverListener.java)
收到记录后自定义进一步处理：
```Java
public interface RetrieverListener {
    public void recordRetrieved(DbRecord record);
    public DbRecord getDbRecord();
}
```
[DbRecord](https://github.com/teclan/teclan-activejdbc/blob/master/src/main/java/teclan/activejdbc/model/DbRecord.java)
```Java
import java.util.LinkedList;
import java.util.List;
import com.google.common.base.Joiner;
import teclan.activejdbc.utils.JsonBuilder;

public class DbRecord {
    private static final JsonBuilder JSON_BUILDER = new JsonBuilder();
    public String        dbName;
    public String        tableName;
    public String        action;
    public List<DbField> dbFields;
    public List<DbField> pkFields;
    private List<DbField> blobFields;
    public DbRecord() {
    }
    public DbRecord(String dbName, String tableName, String action,
            List<DbField> pkFields, List<DbField> dbFields) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.action = action;
        this.pkFields = pkFields;
        this.dbFields = dbFields;
    }
    public List<DbField> getDbFields() {
        return dbFields;
    }
    public void setDbFields(List<DbField> dbFields) {
        this.dbFields = dbFields;
    }
    public List<DbField> getPkFields() {
        return pkFields;
    }
    public void setPkFields(List<DbField> pkFields) {
        this.pkFields = pkFields;
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public final String getDbName() {
        return dbName;
    }
    public final void setDbName(String dbName) {
        this.dbName = dbName;
    }
    public String getAction() {
        return action;
    }
    public void setAction(String action) {
        this.action = action;
    }
    public boolean isDeletedRecord() {
        return getAction().equalsIgnoreCase("DELETE");
    }
    public int getFieldLength() {
        return getPkFields().size() + getDbFields().size();
    }
    public List<DbField> getBlobFields() {
        if (blobFields == null) {
            blobFields = new LinkedList<DbField>();
            for (DbField dbField : getPkFields()) {
                if (dbField.isBlobField()) {
                    blobFields.add(dbField);
                }
            }
            for (DbField dbField : getDbFields()) {
                if (dbField.isBlobField()) {
                    blobFields.add(dbField);
                }
            }
        }
        return blobFields;
    }
    public String toJson() {
        return JSON_BUILDER.build(this);
    }
    @Override
    public String toString() {
        return String.format("库名：%s；表名：%s；主键：%s", getDbName(), getTableName(),
                Joiner.on(",").join(getPkFields()));
    }
    public String getDetailString() {
        return String.format("库名：%s；表名：%s；主键域：%s;数据域:%s", getDbName(),
                getTableName(), Joiner.on(",").join(getPkFields()),
                Joiner.on(",").join(getDbFields()));
    }

}
```  
## 获取表字段类型
```Java
Map<String, String> getTableColumns(String tableName);
```
## 获取表主键
```Java
List<String> getPkNames(String table);
```
## 执行sql(默认连接“default”)
```Java
void execute(String sql);
```
## 执行sql(指定连接)
```Java
void execute(String name, String sql);
```
## 执行sql文件(默认连接"default")
关于sql文件，sql文件中各sql组用#隔开
```Java
void execute(File file);
```
## 执行sql文件(指定连接)
```Java
void execute(String name, File file);
```



 