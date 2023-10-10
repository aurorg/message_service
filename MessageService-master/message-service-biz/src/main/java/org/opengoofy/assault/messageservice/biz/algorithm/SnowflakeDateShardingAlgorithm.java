/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opengoofy.assault.messageservice.biz.algorithm;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Range;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;
import org.opengoofy.assault.framework.starter.distributedid.SnowflakeIdUtil;

import java.util.*;

/**
 * 自定义分片算法
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public final class SnowflakeDateShardingAlgorithm implements ComplexKeysShardingAlgorithm<Date> {

    // 定义分片键的列名
    private final String messageSendId = "msg_id";
    
    private final String sendTime = "create_time";
    
    @Override
    public Collection<String> doSharding(Collection availableTargetNames, ComplexKeysShardingValue shardingValue) {

        // 获取逻辑表名
        String logicTableName = shardingValue.getLogicTableName();

        // 获取分片键和分片值的映射
        Map<String, Collection<Comparable<?>>> columnNameAndShardingValuesMap = shardingValue.getColumnNameAndShardingValuesMap();

        // 获取分片键和范围值的映射
        Map<String, Range<Comparable<?>>> columnNameAndRangeValuesMap = shardingValue.getColumnNameAndRangeValuesMap();

        // 存储分片结果的集合
        Collection<String> result = new LinkedHashSet<>(availableTargetNames.size());

        // 根据分片键"create_time"进行分片
        if (CollUtil.isNotEmpty(columnNameAndShardingValuesMap)) {
            Collection<Comparable<?>> sendTimeCollection = columnNameAndShardingValuesMap.get(sendTime);

            if (CollUtil.isNotEmpty(sendTimeCollection)) {

                // 获取第一个分片值
                Comparable<?> comparable = sendTimeCollection.stream().findFirst().get();

                // 根据具体的分片逻辑计算实际表名
                String actualTable = ShardModel.quarterlyModel(logicTableName, (Date) comparable);
                result.add(actualTable);
            } else {
                Collection<Comparable<?>> messageSendIdCollection = columnNameAndShardingValuesMap.get(messageSendId);
                Comparable<?> comparable = messageSendIdCollection.stream().findFirst().get();

                // 判断消息ID的类型并解析
                Long snowflakeId;
                if (comparable instanceof String) {
                    snowflakeId = Long.parseLong((String) comparable);
                } else if (comparable instanceof Long) {
                    snowflakeId = (Long) comparable;
                } else {
                    throw new RuntimeException("消息ID类型输入错误，请检查");
                }

                // 根据具体的分片逻辑计算实际表名
                String actualTable = ShardModel.quarterlyModel(logicTableName, new Date(SnowflakeIdUtil.parseSnowflakeId(snowflakeId).getTimestamp()));
                result.add(actualTable);
            }
        } else {
            // 根据范围进行分片
            Range<Comparable<?>> sendTimeRange = columnNameAndRangeValuesMap.get(sendTime);
            if (sendTimeRange != null) {

                // 根据范围计算实际表名集合
                Set<String> actualTables = ShardModel.calculateRange(logicTableName, (Date) sendTimeRange.lowerEndpoint(), (Date) sendTimeRange.upperEndpoint());
                result.addAll(actualTables);
            } else {
                // 没有指定分片值或范围，返回所有可用的目标表名
                result.addAll(availableTargetNames);
            }
        }
        return result;
    }

    /*
获取逻辑表名：通过 shardingValue.getLogicTableName() 获取到逻辑表名。
获取分片键和分片值的映射：通过 shardingValue.getColumnNameAndShardingValuesMap() 获取分片键和对应的分片值的映射。
获取分片键和范围值的映射：通过 shardingValue.getColumnNameAndRangeValuesMap() 获取分片键和对应的范围值的映射。
创建存储分片结果的集合：使用 LinkedHashSet 来保留有序性并去重。
判断是否存在分片值的映射：
    如果存在分片值的映射，根据分片键"create_time"进行分片：
        从映射中获取分片键"create_time"对应的分片值集合 sendTimeCollection。
        如果分片值集合不为空，取集合中的第一个值 comparable。
        根据具体的分片逻辑计算实际表名 actualTable，并将其添加到结果集合中。
    如果不存在分片值的映射，根据分片键"msg_id"进行分片：
        从映射中获取分片键"msg_id"对应的分片值集合 messageSendIdCollection。
        取集合中的第一个值 comparable。
        判断消息ID的类型，解析为 snowflakeId。
        根据具体的分片逻辑计算实际表名 actualTable，并将其添加到结果集合中。
判断是否存在范围值的映射：
    如果存在范围值的映射，根据范围进行分片：
        获取范围的下界和上界，即 sendTimeRange.lowerEndpoint() 和 sendTimeRange.upperEndpoint()。
        根据范围计算实际表名集合 actualTables，并将其添加到结果集合中。
    如果不存在范围值的映射，返回所有可用的目标表名。
返回分片结果集合。


分 分片值和范围值两个的原因是：
        设计分片值和范围值的原因在于应对不同的查询需求。有些查询可能是基于特定的ID（例如用户ID）进行的，这时候使用分片值更为合适。
        而有些查询可能涉及到一段时间内的数据，比如按月份或日期范围查询，这时候使用范围值更为方便。
     */
    
    @Override
    public Properties getProps() {
        return null;
    }
    
    @Override
    public void init(Properties properties) {
        
    }
    
    @Override
    public String getType() {
        return "CLASS_BASED";
    }
}
