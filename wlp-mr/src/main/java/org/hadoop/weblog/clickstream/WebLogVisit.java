package org.hadoop.weblog.clickstream;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hadoop.weblog.bean.PageViewsBean;
import org.hadoop.weblog.bean.PageVisitBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: YICHEN
 * @Date: 2020/7/21 16:04
 */
public class WebLogVisit extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);

	/*	String inputPath = "hdfs://node01:8020/weblog/"+ DateUtil.getYestDate() + "/pageViewOut";
		String outPutPath="hdfs://node01:8020/weblog/"+ DateUtil.getYestDate() + "/clickStreamVisit";
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"),conf);
		if (fileSystem.exists(new Path(outPutPath))){
			fileSystem.delete(new Path(outPutPath),true);
		}
		fileSystem.close();
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);*/


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:////Users/CHENYI/Desktop/Java/WebLogParser/wlp-mr/weblog/pageViewsOut"));
        TextOutputFormat.setOutputPath(job,new Path("file:////Users/CHENYI/Desktop/Java/WebLogParser/wlp-mr/weblog/pagevisitOut"));

        job.setJarByClass(WebLogVisit.class);
        job.setMapperClass(ClickStreamVisitMapper.class);
        job.setReducerClass(ClickStreamVisitReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewsBean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PageVisitBean.class);
        boolean res = job.waitForCompletion(true);
        return res?0:1;
    }

    static class ClickStreamVisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            int step = Integer.parseInt(fields[5]);
            PageViewsBean pageViewsBean = new PageViewsBean();
            pageViewsBean.set(fields[0], fields[1], fields[2], fields[3],fields[4], step, fields[6], fields[7], fields[8], fields[9]);
            Text k = new Text();
            k.set(pageViewsBean.getSession());
            context.write(k, pageViewsBean);
        }
    }

    static class ClickStreamVisitReducer extends Reducer<Text, PageViewsBean, NullWritable, PageVisitBean>{
        NullWritable k = NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {
            List<PageViewsBean> pageViewsBeanList = new ArrayList<>();
            for(PageViewsBean bean : values){
                PageViewsBean pageViewsBean = new PageViewsBean();
                try {
                    BeanUtils.copyProperties(pageViewsBean, bean);
                    pageViewsBeanList.add(pageViewsBean);
                }catch (Exception e){
                    continue;
                }
            }
            Collections.sort(pageViewsBeanList, new Comparator<PageViewsBean>() {
                @Override
                public int compare(PageViewsBean o1, PageViewsBean o2) {
                    return o1.getStep() > o2.getStep() ? 1 : -1;
                }
            });
            PageVisitBean pageVisitBean = new PageVisitBean();
            pageVisitBean.setProperties(key.toString(), pageViewsBeanList.get(0).getRemote_addr(), pageViewsBeanList.get(0).getTimestr(), pageViewsBeanList.get(pageViewsBeanList.size()-1).getTimestr(),
                    pageViewsBeanList.get(0).getRequest(), pageViewsBeanList.get(pageViewsBeanList.size()-1).getRequest(),
                    pageViewsBeanList.get(0).getReferal(), pageViewsBeanList.size());
            context.write(k, pageVisitBean);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new WebLogVisit(), args);
        System.exit(run);
    }
}
