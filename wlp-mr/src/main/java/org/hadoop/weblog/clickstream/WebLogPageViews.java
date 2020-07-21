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
import org.hadoop.weblog.bean.WebLogBean;
import org.hadoop.weblog.pre.WeblogPreProcess;
import org.hadoop.weblog.utils.DateUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * @Author: YICHEN
 * @Date: 2020/7/21 14:51
 */
public class WebLogPageViews extends Configured implements Tool {

    public static final String TRUE = "true";
    public static final String FALSE = "false";

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);

	/*	String inputPath="hdfs://node01:8020/weblog/"+DateUtil.getYestDate()+"/weblogPreOut";
		String outputPath="hdfs://node01:8020/weblog/"+DateUtil.getYestDate()+"/pageViewOut";
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), conf);
		if (fileSystem.exists(new Path(outputPath))){
			fileSystem.delete(new Path(outputPath),true);
		}
		fileSystem.close();
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));*/


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:////Users/CHENYI/Desktop/Java/WebLogParser/wlp-mr/weblog/preout"));
        TextOutputFormat.setOutputPath(job,new Path("file:////Users/CHENYI/Desktop/Java/WebLogParser/wlp-mr/weblog/pageViewsOut"));

        job.setJarByClass(WebLogPageViews.class);
        job.setMapperClass(pageViewsMapper.class);
        job.setReducerClass(pageViewsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    static class pageViewsMapper extends Mapper<LongWritable, Text, Text, WebLogBean>{
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
            String[] lineSplits = line.split(",");
            if(lineSplits.length < 9){
                return;
            }
            WebLogBean webLogBean = new WebLogBean();
            webLogBean.set(TRUE.equals(lineSplits[0])?true:false, lineSplits[1], lineSplits[2], lineSplits[3],
                    lineSplits[4], lineSplits[5], lineSplits[6], lineSplits[7], lineSplits[8]);
            if(webLogBean.isValid()){
                k.set(webLogBean.getRemoteAddr());
                context.write(k, webLogBean);
            }
        }
    }

    static class pageViewsReducer extends Reducer<Text, WebLogBean, NullWritable, Text>{
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
            //同一个key分配到同一个reducer
            ArrayList<WebLogBean> beans = new ArrayList<WebLogBean>();
            // 先将一个用户的所有访问记录中的时间拿出来排序
            try {
                //循环遍历V2，这里面装的，都是我们的同一个用的数据
                for (WebLogBean bean : values) {
                    //	beans.add(bean);
                    //为什么list集合当中不能直接添加循环出来的这个bean？
                    //这里通过属性拷贝，每次new  一个对象，避免了bean的属性值每次覆盖
                    //这是涉及到java的深浅拷贝问题
                    WebLogBean webLogBean = new WebLogBean();
                    try {
                        BeanUtils.copyProperties(webLogBean, bean);
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    //beans.add(bean);
                    beans.add(webLogBean);
                }
                //将bean按时间先后顺序排序，排好序之后，就计算这个集合当中下一个时间和上一个时间的差值 ，如
                //如果差值小于三十分钟，那么就代表一次会话，如果差值大于30分钟，那么就代表多次会话
                //将我们的weblogBean塞到一个集合当中，我们就可以自定义排序，对集合当中的数据进行排序
                Collections.sort(beans, new Comparator<WebLogBean>() {
                    @Override
                    public int compare(WebLogBean o1, WebLogBean o2) {
                        try {
                            Date d1 = DateUtils.toDate(o1.getTimeLocal());
                            Date d2 = DateUtils.toDate(o2.getTimeLocal());
                            if (d1 == null || d2 == null)
                                return 0;
                            return d1.compareTo(d2);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return 0;
                        }
                    }

                });

                /**
                 * 以下逻辑为：从有序bean中分辨出各次visit，并对一次visit中所访问的page按顺序标号step
                 * 核心思想：
                 * 就是比较相邻两条记录中的时间差，如果时间差<30分钟，则该两条记录属于同一个session
                 * 否则，就属于不同的session
                 *
                 */

                int step = 1;
                //定义一个uuid作为我们的session编号
                String session = UUID.randomUUID().toString();
                ///经过排序之后，集合里面的数据都是按照时间来排好序了
                for (int i = 0; i < beans.size(); i++) {
                    WebLogBean bean = beans.get(i);
                    // 如果仅有1条数据，则直接输出
                    if (1 == beans.size()) {

                        // 设置默认停留时长为60s
                        v.set(session+","+key.toString()+","+bean.getRemoteUser() + "," + bean.getTimeLocal() + "," + bean.getRequest() + "," + step + "," + (60) + "," + bean.getHttpReferer() + "," + bean.getHttpUserAgent() + "," + bean.getBodyBytesSent() + ","
                                + bean.getStatus());
                        context.write(NullWritable.get(), v);
                        session = UUID.randomUUID().toString();
                        break;
                    }

                    // 如果不止1条数据，则将第一条跳过不输出，遍历第二条时再输出
                    if (i == 0) {
                        continue;
                    }
                    // 求近两次时间差
                    long timeDiff = DateUtils.timeDiff(DateUtils.toDate(bean.getTimeLocal()), DateUtils.toDate(beans.get(i - 1).getTimeLocal()));
                    // 如果本次-上次时间差<30分钟，则输出前一次的页面访问信息
                    if (timeDiff < 30 * 60 * 1000) {

                        v.set(session+","+key.toString()+","+beans.get(i - 1).getRemoteUser() + "," + beans.get(i - 1).getTimeLocal() + "," + beans.get(i - 1).getRequest() + "," + step + "," + (timeDiff / 1000) + "," + beans.get(i - 1).getHttpReferer() + ","
                                + beans.get(i - 1).getHttpUserAgent() + "," + beans.get(i - 1).getBodyBytesSent() + "," + beans.get(i - 1).getStatus());
                        context.write(NullWritable.get(), v);
                        step++;
                    } else {
                        // 如果本次-上次时间差>30分钟，则输出前一次的页面访问信息且将step重置，以分隔为新的visit
                        v.set(session+","+key.toString()+","+beans.get(i - 1).getRemoteUser() + "," + beans.get(i - 1).getTimeLocal() + "," + beans.get(i - 1).getRequest() + "," + (step) + "," + (60) + "," + beans.get(i - 1).getHttpReferer() + ","
                                + beans.get(i - 1).getHttpUserAgent() + "," + beans.get(i - 1).getBodyBytesSent() + "," + beans.get(i - 1).getStatus());
                        context.write(NullWritable.get(), v);
                        // 输出完上一条之后，重置step编号
                        step = 1;
                        session = UUID.randomUUID().toString();
                    }

                    // 如果此次遍历的是最后一条，则将本条直接输出
                    if (i == beans.size() - 1) {
                        // 设置默认停留市场为60s
                        v.set(session+","+key.toString()+","+bean.getRemoteUser() + "," + bean.getTimeLocal() + "," + bean.getRequest() + "," + step + "," + (60) + "," + bean.getHttpReferer() + "," + bean.getHttpUserAgent() + "," + bean.getBodyBytesSent() + "," + bean.getStatus());
                        context.write(NullWritable.get(), v);
                    }
                }

            } catch (ParseException e) {
                e.printStackTrace();

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new WebLogPageViews(), args);
        System.exit(run);
    }
}
