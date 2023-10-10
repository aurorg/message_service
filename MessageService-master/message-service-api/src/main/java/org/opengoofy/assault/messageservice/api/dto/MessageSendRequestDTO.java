package org.opengoofy.assault.messageservice.api.dto;

import lombok.Data;

import java.util.List;

/**
 * 消息发送入参实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
public class MessageSendRequestDTO {
    
    /**
     * 模板ID
     */
    private String templateId;
    
    /**
     * 来源ID
     */
    private String sourceId;
    
    /**
     * 模板类型 0：短信-验证码 1：短信-通知 2：短信-营销 3：微信模板消息 4：邮箱 5...
     */
    private Integer msgType;
    
    /**
     * 接收者
     */
    private String receiver;
    
    /**
     * 模板参数集合
     */
    private List<String> paramList;
    
    /**
     * 客户端配置 MQ 回调
     */
    private MQCallbackDTO mqCallback;
}
