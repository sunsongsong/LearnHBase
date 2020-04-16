package com.happyvicky.hbase.demo3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HDFSReadMapper  extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{

    /**
     * 文本内容：
     * 0007 zhangsan    18
     * 0008 lisi    25
     * 0009 wangwu  20
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        Put put = new Put(split[0].getBytes());
        put.addColumn("f1".getBytes(),"name".getBytes(),split[1].getBytes());
        put.addColumn("f1".getBytes(),"age".getBytes(),split[2].getBytes());

        context.write(new ImmutableBytesWritable(split[0].getBytes()),put);


    }
}
