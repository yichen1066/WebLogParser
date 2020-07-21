package org.hadoop.weblog.pre;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

import org.hadoop.weblog.bean.WebLogBean;


public class WebLogParser {

	public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

	public static WebLogBean parser(String line) {
		WebLogBean webLogBean = new WebLogBean();
		String[] arr = line.split(" ");
		if (arr.length > 11) {
			webLogBean.setRemoteAddr(arr[0]);
			webLogBean.setRemoteUser(arr[1]);
			String time_local = formatDate(arr[3].substring(1));
			if(null==time_local || "".equals(time_local)) time_local="-invalid_time-";
			webLogBean.setTimeLocal(time_local);
			webLogBean.setRequest(arr[6]);
			webLogBean.setStatus(arr[8]);
			webLogBean.setBodyBytesSent(arr[9]);
			webLogBean.setHttpReferer(arr[10]);

			//如果useragent元素较多，拼接useragent
			if (arr.length > 12) {
				StringBuilder sb = new StringBuilder();
				for(int i=11;i<arr.length;i++){
					sb.append(arr[i]);
				}
				webLogBean.setHttpUserAgent(sb.toString());
			} else {
				webLogBean.setHttpUserAgent(arr[11]);
			}

			if (Integer.parseInt(webLogBean.getStatus()) >= 400) {// 大于400，HTTP错误
				webLogBean.setValid(false);
			}
			
			if("-invalid_time-".equals(webLogBean.getTimeLocal())){
				webLogBean.setValid(false);
			}
		} else {
			webLogBean=null;
		}

		return webLogBean;
	}

	public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
		if (bean != null && !pages.contains(bean.getRequest())) {
			bean.setValid(false);
		}
	}
        //格式化时间方法
	public static String formatDate(String time_local) {
		try {
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			return null;
		}

	}

}