package org.opengoofy.assault.messageservice.biz.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 消息实际发送三方平台响应实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessagePlatformSendResponseDTO {
    
    /**
     * 调用三方平台是否成功
     */
    private Boolean success;
    
    /**
     * 三方平台返回码
     */
    private String code;
    
    /**
     * 三方平台返回错误信息
     */
    private String errMsg;
}
