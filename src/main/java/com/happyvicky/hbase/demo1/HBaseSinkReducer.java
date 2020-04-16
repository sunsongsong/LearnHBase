package com.happyvicky.hbase.demo1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 负责将数据写入到myuser2
 *
 */
public class HBaseSinkReducer extends TableReducer<Text,Put,ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        ImmutableBytesWritable keyBytes = new ImmutableBytesWritable(key.toString().getBytes());
        for (Put put : values) {
            context.write(keyBytes,put);
        }
    }
}
