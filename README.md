

  提供主流数据库（Oracle9i+,SqlServer2k+,mysql5.6+,DB28.5+,达梦7+，金仓）的常见操作支持，
 
  常见的数据库操作相关SQL，请点击[这里](https://teclan.github.io/2016/07/15/%E5%B8%B8%E8%A7%81%E6%95%B0%E6%8D%AE%E5%BA%93%E6%93%8D%E4%BD%9CSQL/)
 

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
 
 