#HBase的相关学习

##HBase的基础学习
```
    test目录下：
    案例1：demo1
    介绍了HBase表的创建、添加数据、查询、过滤器的使用、分页功能的实现、建表设置分区
    
    main目录下：
    案例1：demo1
    介绍了Hbase与MR的结合，将HBase表myuser的数据通过MR写入HBase表myuser2中
    
    案例2：demo2
    介绍了Hbase与MR的结合，将HDFS中的数据通过MR写入HBase表myuser2中
    
    案例3：demo3
    介绍了Hbase与MR的结合，将HDFS中的数据通过MR'批量'写入HBase
    方法：直接把HDFS的文件，写成HFile,然后将HFlie加载到HBase中去
    向HBase写入需要经过:HLog + Region(memoryStore -> storeFile) -> HFlie
    
    案例4：demo4
    介绍了HBase协处理器的使用，通过协处理器，将写入表1的数据同时也往表2写了一份
    
    案例5：demo5
    介绍了HBase的版本和过期时间的设置
    
```

##HBase的微博案例学习
```
    步骤一:拷贝HBase服务器上conf目录下的 core-site.xml hbase-site.xml hdfs-site.xml文件到resource下
    
    步骤二：创建命名空间 weibo ,以及三张表 
        微博的内容表  content
        用户关系表  relation
        存储用户发送的微博rowkey receive_content_email
        
    步骤三：实现功能
        发送微博内容
        添加关注用户
        取消关注用户
        获取关注人的微博内容
```
