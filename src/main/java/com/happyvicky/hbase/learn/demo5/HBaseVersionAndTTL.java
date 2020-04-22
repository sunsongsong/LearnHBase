package com.happyvicky.hbase.learn.demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseVersionAndTTL {

    public static void main(String[] args) throws IOException, InterruptedException {
        //操作hbase，向hbase表当中添加一条数据，并且设置数据的上界以及下界，以及设置数据的TTL过期时间

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //创建hbase表
        Admin admin = connection.getAdmin();
        //判断如果hbase表不存在，那么就创建
        if(!admin.tableExists(TableName.valueOf("version_hbase"))){
            //指定表名
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("version_hbase"));
            //为表指定列族名
            HColumnDescriptor f1 = new HColumnDescriptor("f1");
            //针对列族设置版本的上界以及版本下界
            f1.setMinVersions(3);
            f1.setMaxVersions(5);  //设置我们版本的上界

            f1.setTimeToLive(30);  //设置我们f1列族下面所有的列，最大的存活时间是30s

            //为我们表添加列族
            hTableDescriptor.addFamily(f1);

            admin.createTable(hTableDescriptor);

        }

        Table version_hbase = connection.getTable(TableName.valueOf("version_hbase"));

        /*Put put = new Put("1".getBytes());
        //如果需要往里面保存多个版本，一定要带上时间戳
        put.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan".getBytes());


        Thread.sleep(1000);

        Put put2 = new Put("1".getBytes());
        //如果需要往里面保存多个版本，一定要带上时间戳
        put2.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan2".getBytes());

        Thread.sleep(1000);

        Put put3 = new Put("1".getBytes());
        //如果需要往里面保存多个版本，一定要带上时间戳
        put3.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan3".getBytes());

        Thread.sleep(1000);

        Put put4 = new Put("1".getBytes());
        //如果需要往里面保存多个版本，一定要带上时间戳
        put4.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan4".getBytes());


        Thread.sleep(1000);

        Put put5 = new Put("1".getBytes());
        //如果需要往里面保存多个版本，一定要带上时间戳
        put5.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan5".getBytes());

        Thread.sleep(1000);

        Put put6 = new Put("1".getBytes());
        //可以针对某一条数据设置过期时间
   //     put6.setTTL(3000);
        //如果需要往里面保存多个版本，一定要带上时间戳
        put6.addColumn("f1".getBytes(),"name".getBytes(),System.currentTimeMillis(),"zhangsan6".getBytes());

        //将所有的数据都put到version_hbase这个表里面去
        version_hbase.put(put);
        version_hbase.put(put2);
        version_hbase.put(put3);
        version_hbase.put(put4);
        version_hbase.put(put5);
        version_hbase.put(put6);


*/

        Get get = new Get("1".getBytes());
        get.setMaxVersions();//如果不带任何参数，表示将数据的所有的版本全部都获取到,如果带上参数，表示我们获取指定个版本的数据

        Result result = version_hbase.get(get);
        //获取所有的cell
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }


        version_hbase.close();
        connection.close();


    }


}
