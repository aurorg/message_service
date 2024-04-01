package org.opengoofy.assault.messageservice.biz.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengoofy.assault.framework.starter.database.BaseDO;

/**
 * 消息发送记录参数实体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@TableName("send_record_extend")
public class SendRecordExtendDO extends BaseDO {
    
    /**
     * id
     */
    private Long id;
    
    /**
     * 消息发送id
     */
    private String msgId;
    
    /**
     * 发送参数
     */
    private String msgParam;
}
