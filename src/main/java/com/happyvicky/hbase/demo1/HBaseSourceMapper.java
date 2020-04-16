package com.happyvicky.hbase.demo1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

/**
 * 负责读取myuser表当中的数据
 * 如果mapper类需要读取hbase表数据，那么我们mapper类需要继承TableMapper这样的一个类
 * 将key2   value2定义成 text  和put类型
 * text里面装rowkey
 * put装我们需要插入的数据
 */

public class HBaseSourceMapper extends TableMapper<Text,Put> {

    /**
     *
     * @param key  rowkey
     * @param value  result对象，封装了我们一条条的数据
     * @param context  上下文对象
     * @throws IOException
     * @throws InterruptedException
     *
     * 需求：读取myuser表当中f1列族下面的name和age列
     *
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
       //获取到rowkey的字节数组
        byte[] bytes = key.get();
        String rowkey = Bytes.toString(bytes);

        Put put = new Put(bytes);

        //获取到所有的cell
        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            //获取cell对应的列族
            byte[] familyBytes = CellUtil.cloneFamily(cell);
            //获取对应的列
            byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
            //这里判断我们只需要f1列族，下面的name和age列
            if(Bytes.toString(familyBytes).equals("f1") && Bytes.toString(qualifierBytes).equals("name") ||  Bytes.toString(qualifierBytes).equals("age")){
                put.add(cell);
            }

        }

        //将数据写出去
        if(!put.isEmpty()){
            context.write(new Text(rowkey),put);
        }


    }
}
