package org.hadoop.weblog.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: YICHEN
 * @Date: 2020/7/21 15:13
 */
public class DateUtils {

    public static String toStr(Date date) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(date);
    }

    public static Date toDate(String timeStr) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.parse(timeStr);
    }

    public static long timeDiff(String time1, String time2) throws ParseException {
        Date d1 = toDate(time1);
        Date d2 = toDate(time2);
        return d1.getTime() - d2.getTime();

    }

    public static long timeDiff(Date time1, Date time2) throws ParseException {
        // date  调用 getTime获取毫秒值
        return time1.getTime() - time2.getTime();

    }
}
