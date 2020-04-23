package com.happyvicky.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: WeiBoHBase
 * @Description:
 * @Author sunsongsong
 * @Date 2020/4/21 21:13
 * @Version 1.0
 */
public class WeiBoHBase {

    public static void main(String[] args) throws IOException {
        WeiBoHBase weiBoHBase = new WeiBoHBase();
//        weiBoHBase.initNameSpace();
//        weiBoHBase.creatTableContent();
//        weiBoHBase.createTableRelations();
//        weiBoHBase.createTableReceiveContentEmails();


    }

    //定义微博的内容表
    private static final byte[] TABLE_CONTENT = Bytes.toBytes("weibo:content");

    //定义用户关系表
    private static final byte[] TABLE_RELATION = Bytes.toBytes("weibo:relation");

    //存储用户发送的微博rowkey
    private static final byte[] TABLE_RECEIVE_CONTENT_EMAIL = Bytes.toBytes("weibo:receive_content_email");


    /**
     * 创建命名空间，以及定义三个表名称
     */
    public void initNameSpace() throws IOException {
        //连接hbase集群
        Connection connection = getConnection();

        //获取客户端管理员对象
        Admin admin = connection.getAdmin();

        //通过管理员对象创建命名空间
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("weibo").addConfiguration("creator", "jim").build();
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();
    }

    private Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    /**
     * 创建微博内容表
     * 方法名	creatTableeContent
     * Table Name	weibo:content
     * RowKey	用户ID_时间戳
     * ColumnFamily	info
     * ColumnLabel	标题,内容,图片
     * Version	1个版本
     *
     * @throws IOException
     */

    public void creatTableContent() throws IOException {
        //获取连接
        Connection connection = getConnection();

        //得到管理员对象
        Admin admin = connection.getAdmin();

        //通过管理员对象来创建表
        if (!admin.tableExists(TableName.valueOf(TABLE_CONTENT))) {
            //定义表名
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));

            //定义列族名
            HColumnDescriptor info = new HColumnDescriptor("info");
            //设置数据版本的上界以及下界
            info.setMaxVersions(1);
            info.setMinVersions(1);
            info.setBlocksize(2048 * 1024);//设置块大小
            info.setBlockCacheEnabled(true);//允许块数据缓存
            // info.setCompressionType(Compression.Algorithm.SNAPPY);//设置hbase数据的压缩

            hTableDescriptor.addFamily(info);

            admin.createTable(hTableDescriptor);

        }
        //关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 创建relation关系表
     * 方法名	createTableRelations
     * Table Name	weibo:relations
     * RowKey	用户ID
     * ColumnFamily	attends、fans
     * ColumnLabel	关注用户ID，粉丝用户ID
     * ColumnValue	用户ID
     * Version	1个版本
     */
    public void createTableRelations() throws IOException {
        //获取连接
        Connection connection = getConnection();

        //获取管理员对象
        Admin admin = connection.getAdmin();

        if (!admin.tableExists(TableName.valueOf(TABLE_RELATION))) {
            //通过管理员对象来创建表
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATION));

            HColumnDescriptor attends = new HColumnDescriptor("attends");//存储关注了哪些人的id

            attends.setBlockCacheEnabled(true);
            attends.setMinVersions(1);
            attends.setMaxVersions(1);
            attends.setBlocksize(2048 * 1024);


            HColumnDescriptor fans = new HColumnDescriptor("fans");//存储用户有哪些粉丝
            fans.setBlockCacheEnabled(true);
            fans.setMinVersions(1);
            fans.setMaxVersions(1);
            fans.setBlocksize(2048 * 1024);

            hTableDescriptor.addFamily(attends);
            hTableDescriptor.addFamily(fans);

            //创建表
            admin.createTable(hTableDescriptor);
        }
        admin.close();
        connection.close();
    }

    /**
     * 创建微博收件箱表
     * 表结构：
     * 方法名	createTableReceiveContentEmails
     * Table Name	weibo:receive_content_email
     * RowKey	用户ID
     * ColumnFamily	info
     * ColumnLabel	用户ID
     * ColumnValue	取微博内容的RowKey
     * Version	1000
     */
    public void createTableReceiveContentEmails() throws IOException {
        //获取连接
        Connection connection = getConnection();

        //获取管理员对象
        Admin admin = connection.getAdmin();

        //通过管理员对象来创建表
        if (!admin.tableExists(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL))) {

            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));

            HColumnDescriptor info = new HColumnDescriptor("info");
            info.setBlockCacheEnabled(true);
            //设置版本保存1000个，就可以将某个人的微博查看一千条
            info.setMinVersions(1000);
            info.setMaxVersions(1000);
            info.setBlocksize(2048 * 1024);
            hTableDescriptor.addFamily(info);

            admin.createTable(hTableDescriptor);
        }
        admin.close();
        connection.close();
    }

    /**
     * 发布微博内容
     * uid表示用户id
     * content：表示发送的微博的内容
     * <p>
     * 第一步：需要将微博的内容保存到content表里面去  content表
     * <p>
     * 第二步：A的粉丝需要查看到A发布的微博内容。需要查看A用户有哪些粉丝。需要查询relation关系表，查找出A用户究竟有哪些粉丝  relation表
     * <p>
     * 第三步：需要给这些粉丝添加A用户微博的rowkey，在receive_content_email 表当中以fans的id作为rowkey，然后以用户发送uid作为列名，用户微博的rowkey作为列值
     */
    public void publishWeibo(String uid, String content) throws IOException {

        //第一步：将发布的微博内容，保存到content表里面去
        Connection connection = getConnection();
        Table table_content = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        //发布微博的rowkey
        String rowkey = uid + "_" + System.currentTimeMillis();

        Put put = new Put(rowkey.getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), System.currentTimeMillis(), content.getBytes());

        table_content.put(put);

        //第二步：查看用户id他的fans有哪些，查询relation表
        Table table_relation = connection.getTable(TableName.valueOf(TABLE_RELATION));

        //查询uid用户有哪些粉丝
        Get get = new Get(uid.getBytes());
        get.addFamily("fans".getBytes());

        Result result = table_relation.get(get);
        //获取所有的列值，都是uid用户对应的粉丝人
        Cell[] cells = result.rawCells();
        if (cells.length <= 0) {
            return;
        }

        //定义list集合，用于保存uid用户所有的粉丝
        List<byte[]> allFans = new ArrayList<byte[]>();
        for (Cell cell : cells) {
            //这里是获取我们这个用户有哪些列，列名就是对应我们的粉丝的用户id
            byte[] bytes = CellUtil.cloneQualifier(cell);
            allFans.add(bytes);
        }

        //第三步：操作recieve_content_email这个表，将用户的所有的粉丝id作为rowkey，然后以用户发送微博的rowkey作为列值，用户id作为列名来保存数据
        Table table_receive_content_email = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));
        //遍历所有的粉丝，以粉丝的id作为rowkey

        List<Put> putFansList = new ArrayList<Put>();

        for (byte[] allFan : allFans) {
            Put put1 = new Put(allFan);
            put1.addColumn("info".getBytes(), uid.getBytes(), System.currentTimeMillis(), rowkey.getBytes());
            putFansList.add(put1);
        }
        table_receive_content_email.put(putFansList);

        table_receive_content_email.close();
        table_content.close();
        table_relation.close();
        connection.close();
    }

    /**
     * 实现关注用户的逻辑
     * A 用户关注了B,C,D这三个用户
     * 第一步：A关注了B,C,D，在weibo:relation表当中attends列族当中，需要记录A用户关注了哪些人，以A用户id作为rowkey，以B,C,D作为列名，
     * 以B,C,D作为列值，保存到attends列族里面去
     * 第二步：B,C,D多了一个粉丝A，在weibo:relation表当中fans列族里面，以B,C,D用户id作为rowkey，然后以A用户id作为列名，A用户id作为列值，保存到fans列族里面去
     *第三步：A用户关注了B,C,D，那么A用户就需要去查看B,C,D发送的微博内容，以B,C,D的用户id作为查询条件，查询weibo:content表，获取发送微博的rowkey，然后保存到
     * receive_content_email表里面去
     *
     */
    public void  addAttends(String uid,String...attends) throws IOException {

        //第一步：用户uid关注了一批人  attends，需要将关系保存起来，保存到weibo:relation表里面去，以用户id作为rowkey，关注了哪些人的id作为
        //列名，关注了哪些人id作为列值，保存到attends里面去
        Connection connection = getConnection();

        //记录A用户关注了哪些人
        Table table_relation = connection.getTable(TableName.valueOf(TABLE_RELATION));

        Put put = new Put(uid.getBytes());
        //循环遍历所有关注的人
        for (String attend : attends) {
            put.addColumn("attends".getBytes(), attend.getBytes(), attend.getBytes());
        }
        table_relation.put(put);

        //第二步：A用户关注了B,C,D这三个人，那么B,C,D这三个人就会多一个粉丝A，以B,C,D uid作为rowkey，将A的uid作为列名，A的uid作为列值，保存到weibo:relation表的fans列族里面去
        for (String attend : attends) {
            Put put1 = new Put(attend.getBytes());
            put1.addColumn("fans".getBytes(), uid.getBytes(), uid.getBytes());
            table_relation.put(put1);
        }


        //第三步：A用户关注了B,C,D那么A用户就要获取B,C,D发送的微博内容的rowkey
        Table table_content = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        //以B,C,D用户的id作为查询条件，查询出B,C,D用户发送的所有的微博的rowkey
        Scan scan = new Scan();
        ArrayList<byte[]> rowkeyBytes = new ArrayList<>();


        for (String attend : attends) {
            //使用用户id+_ 来作为扫描条件，将所有满足条件的数据全部都扫描出来
            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(rowFilter);
            ResultScanner scanner = table_content.getScanner(scan);
            for (Result result : scanner) {
                //获取B,C,D发送的微博的内容的rowkey
                byte[] row = result.getRow();
                rowkeyBytes.add(row);
            }


        }

        ArrayList<Put> recPuts = new ArrayList<>();

        //第三步：获取到了所有的B,C,D发送的微博的rowkey，将这些rowkey保存到A用户的收件箱表里面去
        if (rowkeyBytes.size() > 0) {
            Table table_receive_content = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));
            //2_158974563
            for (byte[] rowkeyByte : rowkeyBytes) {
                Put put1 = new Put(uid.getBytes());
                String rowKeyStr = Bytes.toString(rowkeyByte);
                //通过截取字符串，获取到用户的uid
                String attendUid = rowKeyStr.substring(0, rowKeyStr.indexOf("_"));
                //用户发送微博的时间戳
                long parseLong = Long.parseLong(rowKeyStr.substring(rowKeyStr.indexOf("_") + 1));
                //将A用户关注的B,C,D用户的微博的rowkey给保存起来
                put1.addColumn("info".getBytes(), attendUid.getBytes(), parseLong, rowkeyByte);
                recPuts.add(put1);

            }
            table_receive_content.put(recPuts);

            table_receive_content.close();
            table_content.close();
            table_relation.close();
            connection.close();
        }
    }

    /**
     * 取消关注逻辑 A 取消关注B,C,D
     * 第一步：在weibo:relation表当中取消关注的用户，将attends列族里面关注的人，都给删除掉
     * 第二步：在weibo:relation表当中减少粉丝，将fans列族里面删除一个A用户
     * 第三步：在weibo:receive_content_email表当中将我们A用户获取B,C,D的关系给删除掉
     */
    public void cancelAttends(String uid,String...attends) throws IOException {
        //第一步：删除关注的人
        Connection connection = getConnection();
        Table table_relation = connection.getTable(TableName.valueOf(TABLE_RELATION));

        //删除A，关注的B,C,D用户
        for (String cancelAttends : attends) {
            Delete delete = new Delete(uid.getBytes());
            delete.addColumn("attends".getBytes(),cancelAttends.getBytes());

            table_relation.delete(delete);
        }


        //第二步：B,C,D这三个用户移除粉丝A
        for (String cancelAttend : attends) {
            Delete delete = new Delete(cancelAttend.getBytes());
            delete.addColumn("fans".getBytes(),uid.getBytes());
            table_relation.delete(delete);
        }

        //第三步：A用户不需要再收到B,C,D发送的微博内容，需要删除receive_content_email表当中对应的rowkey数据

        Table table_receive_content_email = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));

        for (String attend : attends) {

            Delete delete = new Delete(uid.getBytes());
            delete.addColumn("info".getBytes(),attend.getBytes());

            table_receive_content_email.delete(delete);

        }

        table_receive_content_email.close();
        table_relation.close();
        connection.close();
    }

    /**
     * 获取关注人的微博内容
     * 例如A用户关注了B,C用户，那么A用户就需要获取B,C用户发送的微博内容
     * B,C发送的微博的内容，存在了weibo:content表里面了
     * A用户需要获取的信息，在receive_content_email表里面
     */
    public  void  getContent(String uid) throws IOException {

        Connection connection = getConnection();
        Table receive_content_email = connection.getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));
        //直接通过rowkey来获取数据
        Get get = new Get(uid.getBytes());
        get.setMaxVersions(5);//设置我们获取数据的最大版本为5个，可以获取到关注用户的最近的5条微博信息

        //将我们所有查询到的rowkey全部都封装到list集合里面去
        ArrayList<Get> rowkeysList = new ArrayList<>();


        Result result = receive_content_email.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneValue(cell);//获取到单元格的值，单元格里面存储的就是我们的rowkey
            Get get1 = new Get(bytes);
            rowkeysList.add(get1);
        }

        //查询weibo:content表里面所有的这些rowkey对应的数据
        Table table_content = connection.getTable(TableName.valueOf(TABLE_CONTENT));

        Result[] results = table_content.get(rowkeysList);
        for (Result result1 : results) {
            //获取微博内容
            byte[] value = result1.getValue("info".getBytes(), "content".getBytes());
            System.out.println(Bytes.toString(value));
        }
    }

}
