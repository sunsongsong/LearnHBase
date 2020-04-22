package com.happyvicky.hbase.learn.demo2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class HBaseWriteReducer extends TableReducer<Text,NullWritable,ImmutableBytesWritable> {

    /**
     * 0007    zhangsan    18
     * 0008    lisi    25
     * 0009    wangwu  20
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String[] split = key.toString().split("\t");

        Put put = new Put(split[0].getBytes());
        put.addColumn("f1".getBytes(),"name".getBytes(),split[1].getBytes());
        put.addColumn("f1".getBytes(),"age".getBytes(),split[2].getBytes());
        //将我们的数据写出去，key3是ImmutableBytesWritable，这个里面装的是rowkey
        //然后将写出去的数据封装到put对象里面去了
        context.write(new ImmutableBytesWritable(split[0].getBytes()),put);

    }
}
