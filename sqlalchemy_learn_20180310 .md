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



