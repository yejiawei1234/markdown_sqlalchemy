# sqlalchemy python下好用的ORM  
## 连接数据库  

``` 
from sqlalalchemy import create_engine 
engine = create_engine('sqlite:///census_nyc.sqlite') 
print(engine.table_names()) 
connection = engine.connect() 
``` 
上面的方法可以连接数据库  

``` 
from sqlalchemy import MetaData, Table 
metadata = MetaData() 
engine = create_engine('sqlite:///census_nyc.sqlite') 
print(engine.table_names()) 
census = Table('census', metadata, autoload=True, autoload_with=engine) 
print(repr(census)) 
``` 
上面的方法可以用来了解census的表的结构  

接下来，进行数据库操作CRUD的最常见部分，先说说“查”  

```
from sqlalchemy import select 
stmt = select([census]) #这里承接上面的部分，census是已经经过reflact操作的表  
stmt = stmt.where(census.cloumns.state == 'New York') #where的基本用法 
ressults = connection.execute(stmt).fetchall() 

for result in results:
    print(result.age, result.sex, result.pop2008) 
    
#in_() 的用法 
stmt = stmt.where(census.columns.state.in_(states)) #这里的states是一个想要筛选出来的国家的list，当然，也可以是其他数据库的表中的一列 

#and_() 的用法 
stmt = stmt.where(
                  and_(census.columns.state == 'California', 
                       census.columns.sex == 'M'
                      )
                  )
#or_() 的用法 
stmt = stmt.where(
                  or_(census.columns.state == 'California', 
                       census.columns.sex == 'M'
                      )
                  )

#order_by() 的用法 
stmt = stmt.order_by(census.columns.state) 

#desc() 的用法 
stmt = stmt.order_by(census.columns.state.desc()) 
或者 
stmt = stmt.order_by(census.columns.state, census.columns.age.desc()) 

#distinct() 和 scalar() 的用法 
stmt = select([func.count(census.columns.state.distinct())]) 
distinct_state_count = connection.execute(stmt).scalar() 

#count() 的用法
stmt = select([census.columns.state, func.count(census.columns.age)]) 
stmt = stmt.group_by(census.columns.state)

#label() 的用法 
pop2008_sum = func.sum(census.columns.pop2008).label('population') 
stmt = select([census.columns.state, pop2008_sum]) 
stmt = stmt.group_by(census.columns.state) 
results = connection.execute(stmt).fetchall() 
print(results[0].keys()) #通过这个方式可以看到results的列名 
``` 

## 把数据库查询的结果转化为 pandas 中的 dataframe  

``` 
import pandas as pd 
df = pd.DataFrame(results) #这里就把results转变为了df
df.columns = results[0].keys() 给df的列，加上列名 
``` 

## 使用加减乘除的方式 

``` 
stmt = select([census.columns.state, (census.columns.pop2008-census.columns.pop2000).label('pop_change')]) 

stmt = stmt.group_by(census.columns.state) 

stmt = stmt.order_by(desc('pop_change')) 

stmt = stmt.limit(5) 

results = connection.execute(stmt).fetchall() 



#或者向下面这样
from sqlalchemy import case, cast, Float

female_pop2000 = func.sum(
    case([
        (census.columns.sex == 'F', census.columns.pop2000)
    ], else_=0))

total_pop2000 = cast(func.sum(census.columns.pop2000), Float)

stmt = select([female_pop2000/total_pop2000* 100])

percent_female = connection.execute(stmt).scalar() 
``` 

## join 的使用方法 
这里需要区分两种情况，就是数据库本身就有预定义好的外键的情况，以及没有外键(或者有外键，但是不想在这个预定义的外键上join)(pps: 如果不想用这个预定义好的那个外键作为join的依据的话，那这个外键存在的意义是什么？)  
先来说第一种情况，本身存在外键的情况 

``` 
stmt = select([census, state_fact])
stmt = stmt.select_from(census.join(state_fact))

``` 
这里的重点是 **select_from** 这个方法，数据库如果本身有设定相应的外键，那么sqlalchemy会自动识别，并在外键上join  

如果不想在存在外键上join，或者原本就不存在外键的话，可以使用 **select_from** 这个函数的第二个参数上传入想要join的列的名称  

``` 
stmt = select([census, state_fact]) 
stmt = stmt.select_from(
    census.join(state_fact, census.columns.state == state_fact.columns.name)) 
#这个地方把两个表想要join的地方传入即可，但是这里是inner join，left join的方式怎么搞呢？
```
## join 多个表的方法 
如果想要实现多个表的join，例如如下的需求  

``` 
SELECT
 trackid,
 tracks.name AS Track,
 albums.title AS Album,
 artists.name AS Artist
FROM
 tracks
INNER JOIN albums ON albums.albumid = tracks.albumid
INNER JOIN artists ON artists.artistid = albums.artistid;
```  
需要在三个表上进行join的操作，如果使用sqlalchemy，可以写为如下的样子  

```
from sqlalchemy import create_engine, MetaData, select, Table, func
from sqlalchemy.sql import and_, or_, between, case, cast
import pandas as pd

engine = create_engine('sqlite://chinook.db')
connection = engine.connect()
metadata = MetaData()
tracks = Table('tracks', metadata, autoload=True, autoload_with=engine)
customers = Table('customers', metadata, autoload=True, autoload_with=engine)
albums = Table('albums', metadata, autoload=True, autoload_with=engine)
artists = Table('artists', metadata, autoload=True, autoload_with=engine)
stmt = select([tracks.columns.TrackId, 
               tracks.columns.Name.label('Track'), 
               albums.columns.Title.label('Title'),
               artists.columns.Name.label('Artist')]).\
                      select_from(tracks.join(albums).join(artists))

results = connection.execute(stmt).fetchall()

df = pd.DataFrame(results)
df.columns = results[0].keys()
```
join 函数支持链式操作(我也是试出来的，google了半天没有一个靠谱的答案)，可以将sqlalchemy的sql语句打印出来对比一下，如果不是在交互式的环境下，就print(str(stmt))，如果是在交互式的环境下，直接str(stmt)，效果如下

```
'SELECT tracks."TrackId", 
tracks."Name" AS "Track", albums."Title" AS "Title", artists."Name" AS "Artist" 
\nFROM tracks 
JOIN albums ON albums."AlbumId" = tracks."AlbumId" 
JOIN artists ON artists."ArtistId" = albums."ArtistId"'

#Python下的字符串的形式，我简单整理为与sql类似的形式，如下
SELECT
    tracks.TrackId
    tracks.Name AS Track
    albums.Title AS Title
    artists.Name AS Artist
FROM
    tracks
JOIN albums ON albums.AlbumId = tracks.AlbumId
JOIN artists ON artists.ArtistId = albums.ArtistId
```

# left join 以及full outer join 的方法
如果想要使用left join的方法，只需要在join的函数上使用isouter=True就可以了，同时，如果想要使用full outer join，只需要在join函数上使用full=True

```
stmt = select([artists.c.ArtistId,
               albums.c.AlbumId]).\
               select_from(artists.join(albums, isouter=True)).\
               where(albums.c.AlbumId == None)
               
#翻译为sql的写法为
SELECT 
    artists.ArtistId, albums.AlbumId 
FROM 
    artists
LEFT OUTER JOIN albums ON artists.ArtistId = albums.ArtistId 
WHERE albums.AlbumId IS NULL

```

## 自表内join (我自己发明的词)  
其实就是需要在一个同一个表上做join，自己和自己的不同列做join的操作，这里就涉及到了需要使用 **alias()** 这个函数  

```
managers = employees.alias() 
stmt = select(
    [managers.columns.name.label('manager'),
     employees.columns.name.label('employee')]
) 
stmt = stmt.where(managers.columns.id == employees.columns.mgr) 
stmt = stmt.order_by(managers.columns.name) 
results = connection.execute(stmt).fetchall() 

#另一个例子  
manager = employees.alias() 
stmt = select([(manager.c.FirstName + manager.c.LastName).label('manager'),
               (employees.c.FirstName + employees.c.LastName).label('direct report')]).\
               select_from(employees.join(manager, manager.c.EmployeeId == employees.c.ReportsTo)).\
               order_by('manager')
               
#再来一个例子
SELECT DISTINCT
 e1.city,
 e1.firstName || ' ' || e1.lastname AS fullname
FROM
 employees e1
INNER JOIN employees e2 ON e2.city = e1.city 
   AND (e1.firstname <> e2.firstname AND e1.lastname <> e2.lastname)
ORDER BY
 e1.city;

e2 = employees.alias() 
stmt = select([employees.c.City.distinct(),
               (employees.c.FirstName + employees.c.LastName).label('fullname')]).\
               select_from(employees.join(e2, e2.c.City == employees.c.City)).\
               where(and_(employees.c.FirstName != e2.c.FirstName,
                          employees.c.LastName != e2.c.LastName)).\
               order_by(employees.c.City)

```  
## having 的用法

这个可以直接上例子 

```
stmt = select([tracks.c. AlbumId,
               tracks.c.Name,
               func.count(tracks.c.TrackId)]).\
               group_by(tracks.c. AlbumId).\
               having(tracks.c. AlbumId == 1)
```

## case 的用法  
这个也可以直接上例子  

```
cc = case(
          [
            (tracks.c.Milliseconds < 60000, 'short'),
            (and_(tracks.c.Milliseconds < 300000,
                  tracks.c.Milliseconds > 6000), 'medium')
          ],else_='long'
          )
stmt = select([tracks.c.TrackId,
               tracks.c.Name,
               cc.label('category')])
```

## union 以及union all的用法  
直接上例子

```
t1 = select([employees.c.FirstName,
             employees.c.LastName])
t2 = select([employees.c.FirstName,
             employees.c.LastName])
stmt = t1.union(t2)

t1 = select([employees.c.FirstName,
             employees.c.LastName])
t2 = select([employees.c.FirstName,
             employees.c.LastName])
stmt = t1.union_all(t2)

```

## 使用fetchmany()函数  
可以使用fetchmany()函数，来控制一次返回的数据的量，如果是在处理一张很大的表的话  

``` 
while more_results:
    partial_results = results_proxy.fetchmany(50)

    if partial_results == []:
        more_results = False

    for row in partial_results:
        if row.state in state_count:
            state_count[row.state] +=1
        else:
            state_count[row.state] = 1 
results_proxy.close() #要显式关闭，这个是重点 
```
## 建立表的方法(仅限于sqlite)  
建立表的方法和reflact的方法很像，只需要把autoload和autoload_with这两个参数替换为需要建立的表的样式，例子如下  

```
from sqlalchemy import Table, Column, String, Integer, Float, Boolean, MetaData

metadata = MetaData()
engine = create_engine('sqlite:///test.db') 
data = Table('data', metadata,
             Column('name', String(255)),
             Column('count', Integer()),
             Column('amount', Float()),
             Column('valid', Boolean())
			  )

metadata.create_all(engine) 
print(repr(metadata.tables['data']))
```
## insert数据的方法  

``` 
from sqlalchemy import insert, select
stmt = insert(data).values(name='Anna', count=1, amount=1000.00, valid=True)
results = connection.execute(stmt)
print(results.rowcount)
stmt = select([data]).where(data.columns.name == 'Anna')
print(connection.execute(stmt).first())  

#insert多条数据的方法  
values_list = [
    {'name': 'Anna', 'count': 1, 'amount': 1000.00, 'valid': True},
    {'name': 'Taylor', 'count': 1, 'amount': 750.00, 'valid': False}
]

stmt = insert(data)
results = connection.execute(stmt, values_list)

#还可以通过读取csv并把数据insert到表中  
stmt = insert(census)

values_list = []
total_rowcount = 0

for idx, row in enumerate(csv_reader):
    #create data and append to values_list
    data = {'state': row[0], 'sex': row[1], 'age': row[2], 'pop2000': row[3],
            'pop2008': row[4]}
    values_list.append(data)

    if idx % 51 == 0:
        results = connection.execute(stmt, values_list)
        total_rowcount += results.rowcount
        values_list = []
```  
## update 的使用方法  

``` 
stmt = update(state_fact).values(fips_state = 36) 
results = connection.execute(stmt) 
```

## delete 的使用方法  

``` 
from sqlalchemy import select, delete 
stmt = delete(census) 
results = connection.execute(stmt) 


#删掉一个表
state_fact.drop(engine)
print(state_fact.exists(engine))

#删掉所有的表
metadata.drop_all(engine) 
print(census.exists(engine)) 
``` 



