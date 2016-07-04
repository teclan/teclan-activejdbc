create table testdb.student (
  id bigint not null AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(56) NOT NULL,
  age int,
  sex varchar(10),
  men varchar(100),
  enty_time DATETIME,
  created_at DATETIME,
  updated_at DATETIME
);#

insert into student(name,age,sex) values ('Declan',23,'man');#
insert into student(name,age,sex) values ('ZhangSan',40,'man');#