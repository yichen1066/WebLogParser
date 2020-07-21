package org.hadoop.weblog.pre;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.weblog.bean.WebLogBean;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author: YICHEN
 * @Date: 2020/7/21 09:57
 */
public class WebLogPreValid {
    static class WebLogPreProcessMapper extends Mapper<LongWritable, Text, Text, WebLogBean>{
        //过滤请求url
        Set<String> pages = new HashSet<>();
        Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            WebLogParser.filtStaticResource(webLogBean, pages);
            if(webLogBean != null && webLogBean.isValid()){
                k.set(webLogBean.getRemoteAddr());
                context.write(k, webLogBean);
            }
        }
    }

    static class webLogPreProcessReducer extends Reducer<Text, WebLogBean, NullWritable, WebLogBean>{
        @Override
        protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
            for (WebLogBean webLogBean : values){
                context.write(NullWritable.get(), webLogBean);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WebLogPreValid.class);

        job.setMapperClass(WebLogPreProcessMapper.class);
        job.setReducerClass(webLogPreProcessReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(WebLogBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		FileInputFormat.setInputPaths(job, new Path("c:/weblog/18"));
//		FileOutputFormat.setOutputPath(job, new Path("c:/weblog/18valid"));


        job.waitForCompletion(true);

    }
}
