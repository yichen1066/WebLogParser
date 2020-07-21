package org.hadoop.weblog.pre;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hadoop.weblog.bean.WebLogBean;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 处理原始日志，过滤出真实pv请求 转换时间格式 对缺失字段填充默认值 对记录标记valid和invalid
 * 
 */

public class WeblogPreProcess extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//Configuration conf = new Configuration();
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf);
		/*String inputPath= "hdfs://node01:9000/weblog/"+DateUtil.getYestDate()+"/input";
		String outputPath="hdfs://node01:9000/weblog/"+DateUtil.getYestDate()+"/weblogPreOut";
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:9000"), conf);
		if (fileSystem.exists(new Path(outputPath))){
			fileSystem.delete(new Path(outputPath),true);
		}
		fileSystem.close();
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
*/
		FileInputFormat.addInputPath(job,new Path("file:////Users/CHENYI/Desktop/Java/WebLogParser/wlp-mr/weblog"));
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job,new Path("file:////Users/CHENYI/Desktop/Java/WebLogParser/wlp-mr/weblog/preout"));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(WeblogPreProcess.class);
		job.setMapperClass(WeblogPreProcessMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		boolean res = job.waitForCompletion(true);
		return res?0:1;
	}

	static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		// 用来存储网站url分类数据
		Set<String> pages = new HashSet<String>();
		Text k = new Text();
		NullWritable v = NullWritable.get();

		/**
		 * 从外部配置文件中加载网站的有用url分类数据 存储到maptask的内存中，用来对日志数据进行过滤
		 */
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
			if (webLogBean != null) {
				// 过滤js/图片/css等静态资源
				WebLogParser.filtStaticResource(webLogBean, pages);
				/* if (!webLogBean.isValid()) return; */
				k.set(webLogBean.toString());
				context.write(k, v);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		int run = ToolRunner.run(configuration, new WeblogPreProcess(), args);
		System.exit(run);
	}

}
