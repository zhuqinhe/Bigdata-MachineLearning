create table t_a(name string,numb int)
row format delimited
fields terminated by ',';

create table t_b(name string,nick string)
row format delimited
fields terminated by ',';

load data local inpath '/root/hivetest/a.txt' into table t_a;
load data local inpath '/root/hivetest/b.txt' into table t_b;


-- 各类join
--1/ 内连接
-- 笛卡尔积
select a.*,b.*
from t_a a inner join t_b b;


-- 指定join条件
select a.*,b.*
from 
t_a a join t_b b on a.name=b.name;

-- 2/ 左外连接（左连接）
select a.*,b.*
from 
t_a a left outer join t_b b on a.name=b.name;


-- 3/ 右外连接（右连接）
select a.*,b.*
from 
t_a a right outer join t_b b on a.name=b.name;

-- 4/ 全外连接
select a.*,b.*
from
t_a a full outer join t_b b on a.name=b.name;


-- 5/ 左半连接
select a.*
from 
t_a a left semi join t_b b on a.name=b.name;


-- 分组聚合查询

-- 针对每一行进行运算
select ip,upper(url),access_time  -- 该表达式是对数据中的每一行进行逐行运算
from t_pv_log;

-- 求每条URL的访问总次数

select url,count(1) as cnts   -- 该表达式是对分好组的数据进行逐组运算
from t_pv_log 
group by url;

-- 求每个URL的访问者中ip地址最大的

select url,max(ip)
from t_pv_log
group by url;

-- 求每个用户访问同一个页面的所有记录中，时间最晚的一条

select ip,url,max(access_time) 
from  t_pv_log
group by ip,url;


-- 分组聚合综合示例
-- 有如下数据
/*
192.168.33.3,http://www.edu360.cn/stu,2017-08-04 15:30:20
192.168.33.3,http://www.edu360.cn/teach,2017-08-04 15:35:20
192.168.33.4,http://www.edu360.cn/stu,2017-08-04 15:30:20
192.168.33.4,http://www.edu360.cn/job,2017-08-04 16:30:20
192.168.33.5,http://www.edu360.cn/job,2017-08-04 15:40:20


192.168.33.3,http://www.edu360.cn/stu,2017-08-05 15:30:20
192.168.44.3,http://www.edu360.cn/teach,2017-08-05 15:35:20
192.168.33.44,http://www.edu360.cn/stu,2017-08-05 15:30:20
192.168.33.46,http://www.edu360.cn/job,2017-08-05 16:30:20
192.168.33.55,http://www.edu360.cn/job,2017-08-05 15:40:20


192.168.133.3,http://www.edu360.cn/register,2017-08-06 15:30:20
192.168.111.3,http://www.edu360.cn/register,2017-08-06 15:35:20
192.168.34.44,http://www.edu360.cn/pay,2017-08-06 15:30:20
192.168.33.46,http://www.edu360.cn/excersize,2017-08-06 16:30:20
192.168.33.55,http://www.edu360.cn/job,2017-08-06 15:40:20
192.168.33.46,http://www.edu360.cn/excersize,2017-08-06 16:30:20
192.168.33.25,http://www.edu360.cn/job,2017-08-06 15:40:20
192.168.33.36,http://www.edu360.cn/excersize,2017-08-06 16:30:20
192.168.33.55,http://www.edu360.cn/job,2017-08-06 15:40:20

*/
-- 建表映射上述数据
create table t_access(ip string,url string,access_time string)
partitioned by (dt string)
row format delimited fields terminated by ',';


-- 导入数据
load data local inpath '/root/hivetest/access.log.0804' into table t_access partition(dt='2017-08-04');
load data local inpath '/root/hivetest/access.log.0805' into table t_access partition(dt='2017-08-05');
load data local inpath '/root/hivetest/access.log.0806' into table t_access partition(dt='2017-08-06');

-- 查看表的分区
show partitions t_access;

-- 求8月4号以后，每天http://www.edu360.cn/job的总访问次数，及访问者中ip地址中最大的
select dt,'http://www.edu360.cn/job',count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job'
group by dt having dt>'2017-08-04';


select dt,max(url),count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job'
group by dt having dt>'2017-08-04';


select dt,url,count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job'
group by dt,url having dt>'2017-08-04';



select dt,url,count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job' and dt>'2017-08-04'
group by dt,url;


-- 求8月4号以后，每天每个页面的总访问次数，及访问者中ip地址中最大的

select dt,url,count(1),max(ip)
from t_access
where dt>'2017-08-04'
group by dt,url;

-- 求8月4号以后，每天每个页面的总访问次数，及访问者中ip地址中最大的，且，只查询出总访问次数>2 的记录
-- 方式1：
select dt,url,count(1) as cnts,max(ip)
from t_access
where dt>'2017-08-04'
group by dt,url having cnts>2;


-- 方式2：用子查询
select dt,url,cnts,max_ip
from
(select dt,url,count(1) as cnts,max(ip) as max_ip
from t_access
where dt>'2017-08-04'
group by dt,url) tmp
where cnts>2;


+----------------+---------------------------------+-----------------------+--------------+--+
|  t_access.ip   |          t_access.url           | t_access.access_time  | t_access.dt  |
+----------------+---------------------------------+-----------------------+--------------+--+

| 192.168.33.46  | http://www.edu360.cn/job        | 2017-08-05 16:30:20   | 2017-08-05   |
| 192.168.33.55  | http://www.edu360.cn/job        | 2017-08-05 15:40:20   | 2017-08-05   |

| 192.168.33.55  | http://www.edu360.cn/job        | 2017-08-06 15:40:20   | 2017-08-06   |
| 192.168.33.25  | http://www.edu360.cn/job        | 2017-08-06 15:40:20   | 2017-08-06   |
| 192.168.33.55  | http://www.edu360.cn/job        | 2017-08-06 15:40:20   | 2017-08-06   |
+----------------+---------------------------------+-----------------------+--------------+


/*
HIVE 中的复合数据类型

*/
-- 数组
-- 有如下数据：
战狼2,吴京:吴刚:龙母,2017-08-16
三生三世十里桃花,刘亦菲:痒痒,2017-08-20
普罗米修斯,苍老师:小泽老师:波多老师,2017-09-17
美女与野兽,吴刚:加藤鹰,2017-09-17

-- 建表映射：
create table t_movie(movie_name string,actors array<string>,first_show date)
row format delimited fields terminated by ','
collection items terminated by ':';

-- 导入数据
load data local inpath '/root/hivetest/actor.dat' into table t_movie;
load data local inpath '/root/hivetest/actor.dat.2' into table t_movie;

-- 查询
select movie_name,actors[0],first_show from t_movie;

select movie_name,actors,first_show
from t_movie where array_contains(actors,'吴刚');

select movie_name
,size(actors) as actor_number
,first_show
from t_movie;


-- 有如下数据：
1,zhangsan,father:xiaoming#mother:xiaohuang#brother:xiaoxu,28
2,lisi,father:mayun#mother:huangyi#brother:guanyu,22
3,wangwu,father:wangjianlin#mother:ruhua#sister:jingtian,29
4,mayun,father:mayongzhen#mother:angelababy,26

-- 建表映射上述数据
create table t_family(id int,name string,family_members map<string,string>,age int)
row format delimited fields terminated by ','
collection items terminated by '#'
map keys terminated by ':';

-- 导入数据
load data local inpath '/root/hivetest/fm.dat' into table t_family;

+--------------+----------------+----------------------------------------------------------------+---------------+--+
| t_family.id  | t_family.name  |                    t_family.family_members                     | t_family.age  |
+--------------+----------------+----------------------------------------------------------------+---------------+--+
| 1            | zhangsan       | {"father":"xiaoming","mother":"xiaohuang","brother":"xiaoxu"}  | 28            |
| 2            | lisi           | {"father":"mayun","mother":"huangyi","brother":"guanyu"}       | 22            |
| 3            | wangwu         | {"father":"wangjianlin","mother":"ruhua","sister":"jingtian"}  | 29            |
| 4            | mayun          | {"father":"mayongzhen","mother":"angelababy"}                  | 26            |
+--------------+----------------+----------------------------------------------------------------+---------------+--+

-- 查出每个人的 爸爸、姐妹
select id,name,family_members["father"] as father,family_members["sister"] as sister,age
from t_family;

-- 查出每个人有哪些亲属关系
select id,name,map_keys(family_members) as relations,age
from  t_family;

-- 查出每个人的亲人名字
select id,name,map_values(family_members) as relations,age
from  t_family;

-- 查出每个人的亲人数量
select id,name,size(family_members) as relations,age
from  t_family;

-- 查出所有拥有兄弟的人及他的兄弟是谁
-- 方案1：一句话写完
select id,name,age,family_members['brother']
from t_family  where array_contains(map_keys(family_members),'brother');


-- 方案2：子查询
select id,name,age,family_members['brother']
from
(select id,name,age,map_keys(family_members) as relations,family_members 
from t_family) tmp 
where array_contains(relations,'brother');


/*  hive数据类型struct

假如有以下数据：
1,zhangsan,18:male:深圳
2,lisi,28:female:北京
3,wangwu,38:male:广州
4,赵六,26:female:上海
5,钱琪,35:male:杭州
6,王八,48:female:南京
*/

-- 建表映射上述数据

drop table if exists t_user;
create table t_user(id int,name string,info struct<age:int,sex:string,addr:string>)
row format delimited fields terminated by ','
collection items terminated by ':';

-- 导入数据
load data local inpath '/root/hivetest/user.dat' into table t_user;

-- 查询每个人的id name和地址
select id,name,info.addr
from t_user;


/*
    条件控制函数：case when
*/

0: jdbc:hive2://localhost:10000> select * from t_user;
+------------+--------------+----------------------------------------+--+
| t_user.id  | t_user.name  |              t_user.info               |
+------------+--------------+----------------------------------------+--+
| 1          | zhangsan     | {"age":18,"sex":"male","addr":"深圳"}    |
| 2          | lisi         | {"age":28,"sex":"female","addr":"北京"}  |
| 3          | wangwu       | {"age":38,"sex":"male","addr":"广州"}    |
| 4          | 赵六           | {"age":26,"sex":"female","addr":"上海"}  |
| 5          | 钱琪           | {"age":35,"sex":"male","addr":"杭州"}    |
| 6          | 王八           | {"age":48,"sex":"female","addr":"南京"}  |
+------------+--------------+----------------------------------------+--+

需求：查询出用户的id、name、年龄（如果年龄在30岁以下，显示年轻人，30-40之间，显示中年人，40以上老年人）
select id,name,
case
when info.age<30 then '青年'
when info.age>=30 and info.age<40 then '中年'
else '老年'
end
from t_user;



--- IF
0: jdbc:hive2://localhost:10000> select * from t_movie;
+---------------------+------------------------+---------------------+--+
| t_movie.movie_name  |     t_movie.actors     | t_movie.first_show  |
+---------------------+------------------------+---------------------+--+
| 战狼2                 | ["吴京","吴刚","龙母"]       | 2017-08-16          |
| 三生三世十里桃花            | ["刘亦菲","痒痒"]           | 2017-08-20          |
| 普罗米修斯               | ["苍老师","小泽老师","波多老师"]  | 2017-09-17          |
| 美女与野兽               | ["吴刚","加藤鹰"]           | 2017-09-17          |
+---------------------+------------------------+---------------------+--+

-- 需求： 查询电影信息，并且如果主演中有吴刚的，显示好电影，否则烂片

select movie_name,actors,first_show,
if(array_contains(actors,'吴刚'),'好片儿','烂片儿')
from t_movie;





-- row_number() over() 函数
-- 造数据：

1,18,a,male
2,19,b,male
3,22,c,female
4,16,d,female
5,30,e,male
6,26,f,female

create table t_rn(id int,age int,name string,sex string)
row format delimited fields terminated by ',';

load data local inpath '/root/hivetest/rn.dat' into table t_rn;

-- 分组标记序号

select * 
from 
(select id,age,name,sex,
row_number() over(partition by sex order by age desc) as rn 
from t_rn) tmp
where rn<3
;


-- 窗口分析函数  sum() over()  ：可以实现在窗口中进行逐行累加

0: jdbc:hive2://localhost:10000> select * from  t_access_amount;
+----------------------+------------------------+-------------------------+--+
| t_access_amount.uid  | t_access_amount.month  | t_access_amount.amount  |
+----------------------+------------------------+-------------------------+--+
| A                    | 2015-01                | 33                      |
| A                    | 2015-02                | 10                      |
| A                    | 2015-03                | 20                      |
| B                    | 2015-01                | 30                      |
| B                    | 2015-02                | 15                      |
| B                    | 2015-03                | 45                      |
| C                    | 2015-01                | 30                      |
| C                    | 2015-02                | 40                      |
| C                    | 2015-03                | 30                      |
+----------------------+------------------------+-------------------------+--+

-- 需求：求出每个人截止到每个月的总额

select uid,month,amount,
sum(amount) over(partition by uid order by month rows between unbounded preceding and current row) as accumulate
from t_access_amount;




-- 自定义函数
/*
有如下json数据：rating.json
{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
{"movie":"661","rate":"3","timeStamp":"978302109","uid":"1"}
{"movie":"914","rate":"3","timeStamp":"978301968","uid":"1"}
{"movie":"3408","rate":"4","timeStamp":"978300275","uid":"1"}

需要导入hive中进行数据分析
*/

-- 建表映射上述数据
create table t_ratingjson(json string);

load data local inpath '/root/hivetest/rating.json' into table t_ratingjson;

想把上面的原始数据变成如下形式：
1193,5,978300760,1
661,3,978302109,1
914,3,978301968,1
3408,4,978300275,1

思路：如果能够定义一个json解析函数，则很方便了
create table t_rate
as
select myjson(json,1) as movie,cast(myjson(json,2) as int) as rate,myjson(json,3) as ts,myjson(json,4) as uid from t_ratingjson;

解决：
hive中如何定义自己的函数：
1、先写一个java类（extends UDF,重载方法public C evaluate(A a,B b)），实现你所想要的函数的功能（传入一个json字符串和一个脚标，返回一个值）
2、将java程序打成jar包，上传到hive所在的机器
3、在hive命令行中将jar包添加到classpath ：    
		hive>add jar /root/hivetest/myjson.jar;
4、在hive命令中用命令创建一个函数叫做myjson，关联你所写的这个java类
		hive> create temporary function myjson as 'cn.edu360.hive.udf.MyJsonParser';




