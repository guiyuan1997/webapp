# -*- coding: utf-8 -*-
__author__ = 'guiyuan'
import aiomysql
import asyncio
import logging; logging.basicConfig(level=logging.INFO)
#创建一个mysql连接池
def log(sql):
    logging.info('SQL:%s' %sql)
async def create_pool(loop, **kw):
    logging.info('create database connecting pool...')
    global __pool
    __pool= await aiomysql.create_pool(
        host=kw.get('host', 'locahost'),
        port=kw.get('port', 3306),
        user=kw.get('user'),
        password=kw.get('password'),
        db=kw.get('db'),
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )
#创建mysql的select方法
async def select(sql, args, size=None):
    global __pool
    async with (await __pool) as conn:
        cursor = await conn.cursor(aiomysql.DictCursor)
        await cursor.execute(sql.replace('?', '%s'), args or ())
        if size:
            result = await cursor.fetchmany(size)
        else:
            result = await cursor.fetchall()
        await cursor.close()
        logging.info('rows returuned: %s' %len(result))
        return result
#因为mysql的INSERT,CREATE,DELETE需要的参数类似，所以用一个方法代替
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
#如果不自动提交事务，就得添加开始事务和结束事务的sql语句，这里就是开始事务
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected
#创建sql的?占位函数
def create_args_string(num):
    tmp = []
    for i in range (num):
        tmp.append('?')
    return ','.join(tmp)
class Field(object):

    def __init__(self, name, column_type, primarykey, default):
        self.name = name
        self.column_type = column_type
        self.primarykey = primarykey
        self.default = default

    '''
    __str__方法触发情况:
    1 当你打印一个对象的时候， 
    2 当你使用%格式化的时候 
    3 str() 强转数据类型的时候
    '''
    def __str__(self):
        return '<%s, %s:%s>' %(self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primarykey=False, default=None, column_type='varchar(100)'):
        super.__init__(name, column_type, primarykey, default)
class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super.__init__(name=name, column_type='boolean', primarykey=False, default=default)
class IntegerField(Field):
    def __init__(self, name=None, primarykey=False, default=0):
        super.__init__(name, 'bigint', primarykey, default)
class FloatField(Field):
    def __init__(self, name=None, primarykey=False, default=0.0):
        super.__init__(name, 'real', primarykey, default)
class TextField(Field):
    def __init__(self, name=None, default=None):
        super.__init__(name, 'text', False, default)

class ModelMetaclass(type):
    '''
    __new__()方法接收到的参数依次是：
    1.当前准备创建的类的对象；
    2.类的名字；
    3.类继承的父类集合；
    4.类的方法集合。
    '''
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        #获取表名
        tablename = attrs.get('__table__', None) or name
        logging.info('found model : %s (table:%s)' %(name, tablename))
        mapping = dict()
        fields = []
        primarykey = None
        for key, value in attrs.items():
            if isinstance(value, Field):
                print('found mapping: %s ---> %s' %(key, value))
                mapping[key]=value
                #找到主键
                if value.primarykey:
                    #如果找到了两次主键，就给一个重复主键的异常
                    if primarykey:
                        raise BaseException('Duplicate primary key for field: %s' % key)
                    primarykey = key
                #如果不是主键，就存入field字段中
                else:
                    fields.append(key)
        #如果遍历完字段都没有找到主键的话
        if not primarykey:
            raise BaseException('Primary Key Not Found')
        #将存入了mapping的字段从attr中删除，以免重复
        for key in mapping.keys():
            attrs.pop(key)
        #防止列名与关键字冲突，定义一个方法，往每个列名上加上sql防冲突符号：``
        escaped_fields = list(map(lambda x: '`%s`' %x, fields))
        attrs['__mapping__']=mapping #定义了用来存放属性与列对应关系的属性
        attrs['__table__']=tablename #用来存放表名的属性
        attrs['__primarykey__']=primarykey #用来存放主键的属性
        attrs['__fields__']=fields #存放除了主键以外的属性名
        attrs['__select__']='select `%s`, %s from `%s`' %(primarykey, ','.join(escaped_fields), tablename)
        attrs['__insert__']='insert into `%s` (%s, `%s`) values (%s)' % (tablename, ','.join(escaped_fields), primarykey, )