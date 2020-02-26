#java 简易数据库
 
##语法
* 支持select
* 支持insert into
* 支持delete
* 支持create table
* 支持drop table
* 支持order by
* 支持group by
* 支持like
* 支持in
* 支持not in
  
##接口
* 支持jdbc
* 支持client

##特性
* 不需要用户
* 可能需要表空间(文件存放路径) 
* 不支持事务
 
##参考命令
```
mydb -h 127.0.0.1 -p 3306
show dbs;
create db [dbname] [dbfile];
use [dbname];
show tables;
create table [tablename] {
    [column_name]……
};
drop table [tablename];
select * from [tablename];
 
```