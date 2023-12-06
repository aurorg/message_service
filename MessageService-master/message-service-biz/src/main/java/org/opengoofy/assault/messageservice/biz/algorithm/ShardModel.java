package org.opengoofy.assault.messageservice.biz.algorithm;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

/**
 * 计算分片表地址
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
public class ShardModel {
    
    public static final String SEND_MSG_SHARD_TABLE = "%s_%d_m%d";  //格式化字符串，用于表示分片表的地址

    /**
     * 根据表名和日期生成分片表的地址
     * @param table 表名
     * @param date 日期
     * @return 分片表的地址
     */
    public static String quarterlyModel(String table, Date date) {  //定义分片表的地址格式化字符串
        int year = DateUtil.year(date);
        int quarter = DateUtil.month(date) + 1;
        return String.format(SEND_MSG_SHARD_TABLE, table, year, quarter);  //将表名、年份和季度填充到分片表的格式化字符串中，并返回结果
    }

    /**
     * 计算在指定时间范围内的分片表地址
     * @param tableName 表名
     * @param start 起始日期
     * @param end 结束日期
     * @return 时间范围内的分片表地址集合
     */
    public static Set<String> calculateRange(String tableName, Date start, Date end) {  //计算在指定时间范围内的分片表地址
        int year = DateUtil.year(start); // 获取起始日期的年份
        Set<String> result = Sets.newHashSet(); // 创建一个用于存储结果的集合
        Calendar calendar = Calendar.getInstance(); // 获取当前时间的 Calendar 实例，并将其设置为起始日期
        calendar.setTime(start);

        while (calendar.getTime().before(end)) { // 在起始日期之前的每个月循环
            int month = calendar.get(Calendar.MONTH); // 获取当前月份
            result.add(String.format(SEND_MSG_SHARD_TABLE, tableName, year, month + 1)); // 将表名、年份和月份填充到分片表的格式化字符串中，并将结果添加到结果集合中
            if (month == 11) {  // 如果当前月份是 11（即 12 月），则年份加1
                year += 1;
            }
            calendar.add(Calendar.MONTH, 1);   // 将日期增加一个月
        }
        Calendar endCalendar = Calendar.getInstance(); // 创建一个 Calendar 实例，并将其设置为结束日期
        endCalendar.setTime(end);

        //上面的循环中处理的是开始日期和结束日期不在同一个月，通过循环处理每一个月份的情况，将每个月份对应的分片表地址都添加到集合中。

        if (calendar.get(Calendar.MONTH) == endCalendar.get(Calendar.MONTH)) { // 如果起始日期的月份与结束日期的月份相同
            int month = calendar.get(Calendar.MONTH);   // 获取当前月份
            result.add(String.format(tableName + "_%d_m%d", year, month + 1));   // 将表名、年份和月份填充到格式化字符串中，并将结果添加到结果集合中
        }
        return result;
    }

    public static void main(String[] args) {
        // 测试精确时间查询
        // 创建一个日期 fakeDate，并调用 quarterlyModel函数打印出精确时间查询的结果
        DateTime fakeDate = DateUtil.parseDate(String.format("%s-%s-%s 00:00:00", 2020, 03, 01));
        System.out.println(quarterlyModel("send_record", fakeDate));

        // 测试范围时间查询
        // 创建一个日期 fakeDate2，并调用 calculateRange 函数打印出范围时间查询的结果
        DateTime fakeDate2 = DateUtil.parseDate(String.format("%s-%s-%s 00:00:00", 2021, 05, 01));
        System.out.println(calculateRange("send_record", fakeDate, fakeDate2));
    }
}
