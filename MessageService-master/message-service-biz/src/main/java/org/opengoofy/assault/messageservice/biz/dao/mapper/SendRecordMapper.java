package org.opengoofy.assault.messageservice.biz.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.opengoofy.assault.messageservice.biz.dao.entity.SendRecordDO;
import org.opengoofy.assault.messageservice.biz.job.receipt.MessageReceiptDTO;

import java.util.List;

/**
 * 消息发送记录持久层
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public interface SendRecordMapper extends BaseMapper<SendRecordDO> {
    
    /**
     * 批量更新短信状态
     */
    @Update({
            "<script>",
            "<foreach collection='list' item= 'item' index ='index' separator=';'>",
            "update ${table} set status = #{item.status}, sender = #{item.sender}, receipt_time = #{item.receiveTime}, update_time = now()",
            "where del_flag = '0' and receiver = #{item.receiver} and msg_id = #{item.msgId}",
            "</foreach>",
            "</script>"
    })
    void batchUpdate(@Param("list") List<MessageReceiptDTO> list, @Param("table") String table);
}
