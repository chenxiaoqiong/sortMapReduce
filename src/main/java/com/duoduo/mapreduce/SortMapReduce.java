package com.duoduo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortMapReduce extends Configured implements Tool {

    public static class SortMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private final static IntWritable ints = new IntWritable(1);
        private IntWritable keyword = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();

            keyword.set(Integer.parseInt(line));

            // void write(KEYOUT var1, VALUEOUT var2) 此方法会按KEYOUT var1自动排序
            context.write(keyword, ints);
        }
    }

    public static class SortReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable linenum  = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {

            for(IntWritable val:value){

                context.write(linenum, key);

                linenum = new IntWritable(linenum.get()+1);

            }
        }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置文件：
        Configuration conf = super.getConf();

        //创建job：
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(SortMapReduce.class);

        //配置作业：
        // Input --> Map --> Reduce --> Output
        // Input:
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        //FileInputFormat过程会将文件处理（Format）成 <偏移量,每一行内容> 的key value对。

        //Map  设置Mapper类，设置Mapper类输出的Key、Value的类型：
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Reduce  设置Reducer类， 设置最终输出的 Key、Value的类型（setOutputKeyClass、setOutputValueClass）：
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Output 设置输出路径
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        //提交任务
        boolean isSucess = job.waitForCompletion(true);
        return isSucess ? 1 : 0;     //成功返回1 ，失败返回0
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new SortMapReduce(), args);
        System.exit(status);
    }
}