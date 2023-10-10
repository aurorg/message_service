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
public class MessageQueryResponseDTO {
    
    /**
     * 消息发送id
     */
    private String msgId;
    
    /**
     * 消息批量发送id
     */
    private String msgBatchId;
    
    /**
     * 模板id
     */
    private String templateId;
    
    /**
     * 模板类型 0：短信-验证码 1：短信-通知 2：短信-营销 3：微信模板消息 4：邮箱 5...
     */
    private Integer msgType;
    
    /**
     * 渠道，用来统计发送来源方
     */
    private String sourceId;
    
    /**
     * 发送者
     */
    private String sender;
    
    /**
     * 接收者
     */
    private String receiver;
    
    /**
     * 抄送者
     */
    private String cc;
    
    /**
     * 短信计费条数
     */
    private Integer billingCount;
    
    /**
     * 消息状态: 0：发送成功 1：发送失败 2：发送中 3：提交失败
     */
    private Integer status;
    
    /**
     * 失败详情
     */
    private String failInfo;
    
    /**
     * 发送时间
     */
    private Date sendTime;
    
    /**
     * 接收时间
     */
    private Date receiptTime;
}
