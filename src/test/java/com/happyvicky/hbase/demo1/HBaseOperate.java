package com.happyvicky.hbase.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseOperate {

    /**
     * 创建hbase表 myuser，带有两个列族 f1  f2
     */
    @Test
    public void createTable() throws IOException {
        //连接hbase集群
        Configuration configuration = HBaseConfiguration.create();
        //指定hbase的zk连接地址
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取管理员对象
        Admin admin = connection.getAdmin();
        //通过管理员对象创建表
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("myuser"));
        //给我们的表添加列族，指定两个列族  f1   f2
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        //将两个列族设置到  hTableDescriptor里面去
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        //创建表
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();

    }

    /***
     * 向表当中添加数据
     */
    @Test
    public  void  addData() throws IOException {
        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表对象
        Table myuser = connection.getTable(TableName.valueOf("myuser"));
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(),"id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("张三"));
        put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(18));
        put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("地球人"));
        put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15845678952"));
        myuser.put(put);
        //关闭表
        myuser.close();

    }

    @Test
    public void insertBatchData() throws IOException {
        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表
        Table myuser = connection.getTable(TableName.valueOf("myuser"));
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(30));
        put.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));


        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<Put>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        myuser.put(listPut);
        myuser.close();
    }


    private  Connection connection;
    private Configuration configuration;

    private Table table;

    /**
     * 初始化的操作
     */
    @BeforeTest
    public void initTable() throws IOException {
        //获取连接
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");

        connection = ConnectionFactory.createConnection(configuration);
        table= connection.getTable(TableName.valueOf("myuser"));
    }


    /**
     * 查询rowkey为0003的人，所有的列
     */
    @Test
    public  void  getData() throws IOException {
        Get get = new Get("0003".getBytes());
        get.addFamily("f1".getBytes());
//        get.addColumn("f1".getBytes(),"id".getBytes());
        //Result是一个对象，封装了我们所有的结果数据
        Result result = table.get(get);
        //获取0003这条数据所有的cell值
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            //获取列族的名称
            String familyName = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            //获取列的名称
            String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            if(familyName.equals("f1") && columnName.equals("id") || columnName.equals("age")){
                int value = Bytes.toInt(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                System.out.println("列族名为"+familyName+"列名为" +  columnName + "列的值为" +  value);
            }else{
                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                System.out.println("列族名为"+familyName+"列名为" +  columnName + "列的值为" +  value);
            }
        }
    }


    /**
     * 按照rowkey进行范围值的扫描
     * 扫描rowkey范围是0004到0006的所有的值
     */
    @Test
    public void scanRange() throws IOException {
        Scan scan = new Scan();
        //设置我们起始和结束rowkey,范围值扫描是包括前面的，不包括后面的
//        scan.setStartRow("0004".getBytes());
//        scan.setStopRow("0006".getBytes());
        //返回多条数据结果值都封装在resultScanner里面了
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                //获取列族名
                String familyName  = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if(familyName.equals("f1") && columnName.equals("id") || columnName.equals("age")){
                    int value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("数据的rowkey为" +  rowkey + "    数据的列族名为" +  familyName + "    列名为" + columnName + "   列值为" +  value);
                }else{
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("数据的rowkey为" +  rowkey + "    数据的列族名为" +  familyName + "    列名为" + columnName + "   列值为" +  value);
                }
            }
        }
    }


    /**
     * 使用rowFilter查询比0003小的所有的数据
     */
    @Test
    public void filterStudy() throws IOException {

        Scan scan = new Scan();

        //查询rowkey比0003小的所有的数据
      //  RowFilter rowFilter = new RowFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("0003")));
      //  scan.setFilter(rowFilter);

        //查询比f2列族小的所有的列族里面的数据

//        FamilyFilter f2 = new FamilyFilter(CompareOperator.LESS, new SubstringComparator("f2"));
//        scan.setFilter(f2);

        //只查询name列的值
      //  QualifierFilter name = new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator("name"));
      //  scan.setFilter(name);

        //查询value值当中包含8的所有的数据
        // ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("8"));
      //  scan.setFilter(valueFilter);

        //查询name值为刘备的数据
        //SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareOperator.EQUAL, "刘备".getBytes());
        //scan.setFilter(singleColumnValueFilter);

        //查询rowkey以00开头所有的数据
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        scan.setFilter(prefixFilter);


        //返回多条数据结果值都封装在resultScanner里面了
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                //获取列族名
                String familyName  = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if(familyName.equals("f1") && columnName.equals("id") || columnName.equals("age")){
                    int value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("数据的rowkey为" +  rowkey + "    数据的列族名为" +  familyName + "    列名为" + columnName + "   列值为" +  value);
                }else{
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("数据的rowkey为" +  rowkey + "    数据的列族名为" +  familyName + "    列名为" + columnName + "   列值为" +  value);
                }
            }
        }

    }


    /**
     * 实现hbase的分页的功能
     */
    @Test
    public void hbasePage() throws IOException {

        int pageNum= 3;
        int pageSize = 2 ;
        if(pageNum == 1){
            Scan scan = new Scan();
            //如果是查询第一页数据，就按照空来进行扫描
            scan.withStartRow("".getBytes());
            PageFilter pageFilter = new PageFilter(pageSize);
            scan.setFilter(pageFilter);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] row = result.getRow();
                System.out.println(Bytes.toString(row));
            }

        }else{

            String  startRow = "";
            //计算我们前两页的数据的最后一条，再加上一条，就是第三页的起始rowkey
            Scan scan = new Scan();
            scan.withStartRow("".getBytes());
            PageFilter pageFilter = new PageFilter((pageNum - 1) * pageSize + 1);
            scan.setFilter(pageFilter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] row = result.getRow();
                startRow = Bytes.toString(row);
            }

            //获取第三页的数据
            scan.withStartRow(startRow.getBytes());
            PageFilter pageFilter1 = new PageFilter(pageSize);
            scan.setFilter(pageFilter1);
            ResultScanner scanner1 = table.getScanner(scan);
            for (Result result : scanner1) {
                byte[] row = result.getRow();
                System.out.println(Bytes.toString(row));
            }
        }
    }


    /**
     * 多过滤器综合查询
     * 需求：使用SingleColumnValueFilter查询f1列族，name为刘备的数据，并且同时满足rowkey的前缀以00开头的数据（PrefixFilter）
     */
    @Test
    public void filterList() throws IOException {

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareOperator.EQUAL, "刘备".getBytes());

        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        //使用filterList来实现多过滤器综合查询
        FilterList filterList = new FilterList(singleColumnValueFilter, prefixFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                //获取列族名
                String familyName  = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if(familyName.equals("f1") && columnName.equals("id") || columnName.equals("age")){
                    int value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("数据的rowkey为" +  rowkey + "    数据的列族名为" +  familyName + "    列名为" + columnName + "   列值为" +  value);
                }else{
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("数据的rowkey为" +  rowkey + "    数据的列族名为" +  familyName + "    列名为" + columnName + "   列值为" +  value);
                }
            }
        }
    }

    /**
     * 根据rowkey删除某一条数据
     */
    @Test
    public   void deleteData() throws IOException {
        Delete delete = new Delete("0007".getBytes());
        table.delete(delete);

    }


    /**
     * 删除表操作
     */
    @Test
    public void deleteTable() throws IOException {
        //获取管理员对象
        Admin admin = connection.getAdmin();
        //禁用表
        admin.disableTable(TableName.valueOf("myuser"));
        //删除表
        admin.deleteTable(TableName.valueOf("myuser"));
    }


    /**
     * 更新操作与插入操作是一模一样的，如果rowkey已经存在那么就更新
     * 如果rowkey不存在，那么就添加
     */
    @Test
    public void updateOperate(){}




    @AfterTest
    public void closeTable() throws IOException {
        table.close();
    }



    /**
     * 通过javaAPI进行HBase的表的创建以及预分区操作
     */
    @Test
    public void hbaseSplit() throws IOException {
        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //自定义算法，产生一系列Hash散列值存储在二维数组中
        byte[][] splitKeys = {{1,2,3,4,5},{'a','b','c','d','e'}};


        //通过HTableDescriptor来实现我们表的参数设置，包括表名，列族等等
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("staff3"));
        //添加列族
        hTableDescriptor.addFamily(new HColumnDescriptor("f1"));
        //添加列族
        hTableDescriptor.addFamily(new HColumnDescriptor("f2"));
        admin.createTable(hTableDescriptor,splitKeys);
        admin.close();

    }








}
