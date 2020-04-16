package com.happyvicky.hbase.demo2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 通过这个mapper读取hdfs上面的文件，然后进行处理
 */
public class HDFSMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //读取到数据之后不做任何处理，直接将数据写入到reduce里面去进行处理
        context.write(value,NullWritable.get());

    }
}
