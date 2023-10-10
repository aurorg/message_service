package org.opengoofy.assault.messageservice.biz.job.receipt;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 三方渠道商短信回执参数
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageReceiptDTO {
    
    /**
     * 发送消息标识 ID
     */
    private String msgId;
    
    /**
     * 接收者
     */
    private String receiver;
    
    /**
     * 发送者. eg. 阿里云、腾讯云...
     */
    private String sender;
    
    /**
     * 发送状态
     */
    private Integer status;
    
    /**
     * 消息接收时间
     */
    private Date receiptTime;
}
