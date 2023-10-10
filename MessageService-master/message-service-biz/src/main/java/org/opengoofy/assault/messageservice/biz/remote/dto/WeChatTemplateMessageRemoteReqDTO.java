package org.opengoofy.assault.messageservice.biz.remote.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 微信模板消息发送远程调用返回实体
 *
 * <p>
 * PS：此处为了容易理解，跳转小程序等参数都没有添加。如需了解访问：
 * <a href="https://developers.weixin.qq.com/doc/offiaccount/Message_Management/Template_Message_Interface.html#%E5%8F%91%E9%80%81%E6%A8%A1%E6%9D%BF%E6%B6%88%E6%81%AF"/>
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WeChatTemplateMessageRemoteReqDTO {
    
    /**
     * 接收者openid
     */
    private String touser;
    
    /**
     * 模板 ID
     */
    private String template_id;
    
    /**
     * 模板数据
     */
    private Map<String, WeChatTemplateMessageDataDTO> data;
    
    /**
     * 防重入id。对于同一个openid + client_msg_id，只发送一条消息，10 分钟有效，超过 10 分钟不保证效果。若无防重入需求，可不填
     */
    private String client_msg_id;
    
    /**
     * {
     * "keyword1":{
     * "value":"巧克力"
     * },
     * "keyword2":{
     * "value":"39.8元"
     * },
     * "keyword3":{
     * "value":"2014年9月22日"
     * }
     * }
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class WeChatTemplateMessageDataDTO {
        
        /**
         * 参数值
         */
        private String value;
    }
}
