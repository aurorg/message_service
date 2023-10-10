package org.opengoofy.assault.messageservice.biz.remote.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 微信模板消息发送远程调用返回实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WeChatTemplateMessageRemoteRespDTO {
    
    /**
     * 错误码
     */
    private Integer errcode;
    
    /**
     * 错误信息
     */
    private String errmsg;
    
    /**
     * 消息 ID
     */
    private Long msgid;
    
    /**
     * 构建微信模板消息发送错误实体对象
     */
    public static WeChatTemplateMessageRemoteRespDTO buildError() {
        return WeChatTemplateMessageRemoteRespDTO.builder()
                .errcode(-1)
                .errmsg("调用微信发送模板消息错误")
                .build();
    }
    
    /**
     * 构建微信模板消息发送返回请求是否正确
     */
    public boolean isSuccess() {
        return this.errcode == 0 ? true : false;
    }
}
