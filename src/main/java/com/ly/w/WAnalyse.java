package com.ly.w;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * “W” 获利点分析
 * @author leiyang
 * @date 2021/4/28 15:22
 */
public class WAnalyse extends Configured implements Tool {

    /**
     * 对输入K线数据，筛选出收盘价，成交量、交易时间，按时间降序
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        private final static Integer max = 80;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 将输入的纯文本文件的数据转化成String
            String line = value.toString();
            // 将输入的数据首先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, ",");
            int i = 0;
            // 分别对每一行进行处理
            while (tokenizerArticle.hasMoreElements() && ++i < max) {
                String nextToken = tokenizerArticle.nextToken();
                //System.out.println(nextToken);
                context.write(new Text(nextToken), one);
            }
        }
    }

    /**
     *
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        // 实现reduce函数
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            context.write(key, result);
        }
    }


    public int run(String[] args) throws Exception {
        //设置输入输出文件目录
        String[] ioArgs = new String[]{"hdfs://192.168.1.181:9000/qa/response.txt", "hdfs://192.168.1.181:9000/qa/data_out"};
        String[] otherArgs = new GenericOptionsParser(getConf(), ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:  <in> <out>");
            System.exit(2);
        }

        //设置一个job
        Job job = Job.getInstance(getConf(), "Quantitative analyse");
        job.setJarByClass(WAnalyse.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(WAnalyse.Map.class);
        job.setCombinerClass(WAnalyse.Reduce.class);
        job.setReducerClass(WAnalyse.Reduce.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);

        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        return job.waitForCompletion(true) ? 0 : 1;

    }


    public static void main(String[] args) throws Exception {
        long l = System.currentTimeMillis();
        Configuration conf = new Configuration();
        //设置hadoop的机器、端口
        conf.set("mapred.job.tracker", "192.168.1.181:8080");
        int res = ToolRunner.run(conf, new WAnalyse(), args);
        System.out.println(System.currentTimeMillis() - l);
        System.exit(res);
    }
}
