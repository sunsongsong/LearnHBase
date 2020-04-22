package com.happyvicky.hbase.learn.demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class MyProcessor implements RegionObserver,RegionCoprocessor {

    static Connection connection = null;
    static Table table = null;
    //使用静态代码块来创建连接对象，避免频繁的创建连接对象
    static{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node01:2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("proc2"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private RegionCoprocessorEnvironment env = null;
    //定义列族名
    private static final String FAMAILLY_NAME = "info";
    //定义列名
    private static final String QUALIFIER_NAME = "name";
    //2.0加入该方法，否则无法生效
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        // Extremely important to be sure that the coprocessor is invoked as a RegionObserver
        return Optional.of(this);
    }

    /**
     * 初始化协处理器环境
     * @param e
     * @throws IOException
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        env = (RegionCoprocessorEnvironment) e;
    }
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // nothing to do here
    }
    /**
     * 覆写prePut方法，在我们数据插入之前进行拦截，
     * @param e
     * @param put  put对象里面封装了我们需要插入到目标表的数据
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit, final Durability durability)
            throws IOException {
        try {
            //通过put对象获取插入数据的rowkey
            byte[] rowBytes = put.getRow();
            String rowkey = Bytes.toString(rowBytes);
            //获取我们插入数据的name字段的值

            List<Cell> list = put.get(Bytes.toBytes(FAMAILLY_NAME), Bytes.toBytes(QUALIFIER_NAME));
            //判断如果没有获取到info列族，和name列，直接返回即可
            if (list == null || list.size() == 0) {
                return;
            }
            //获取到info列族，name列对应的cell
            Cell cell2 = list.get(0);

            //通过cell获取数据值
            String nameValue = Bytes.toString(CellUtil.cloneValue(cell2));
            //创建put对象，将数据插入到proc2表里面去
            Put put2 = new Put(rowkey.getBytes());
            put2.addColumn(Bytes.toBytes(FAMAILLY_NAME), Bytes.toBytes(QUALIFIER_NAME),  nameValue.getBytes());
            table.put(put2);
            table.close();
        } catch (Exception e1) {
            return ;
        }
    }
}

