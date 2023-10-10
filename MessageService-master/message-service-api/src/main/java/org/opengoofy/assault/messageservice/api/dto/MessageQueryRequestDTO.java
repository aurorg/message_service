package org.opengoofy.assault.messageservice.api.dto;

import lombok.Data;

import java.util.Date;

/**
 * 消息查询入参实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
public class MessageQueryRequestDTO {
    
    /**
     * 接收者，必填
     */
    private String receiver;
    
    /**
     * 消息 ID
     */
    private String msgId;
    
    /**
     * 发送开始时间
     */
    private Date sendStartTime;
    
    /**
     * 发送结束时间
     */
    private Date sendEndTime;
}
