package org.opengoofy.assault.messageservice.biz.job.receipt.impl;

import lombok.Data;

import java.util.List;

@Data
public class AliSmsMessageReceiptRespDTO {
    
    /**
     * 请求状态码
     */
    private String Code;
    
    /**
     * 状态码的描述
     */
    private String Message;
    
    /**
     * 请求ID
     */
    private String RequestId;
    
    /**
     * 短信发送总条数
     */
    private String TotalCount;
    
    /**
     * 短信发送明细
     */
    private List<SmsSendDetailDTO> SmsSendDetailDTOs;
    
    @Data
    public static class SmsSendDetailDTO {
        
        /**
         * 运营商短信状态码
         */
        private String ErrCode;
        
        /**
         * 短信模板ID
         */
        private String TemplateCode;
        
        /**
         * 外部流水扩展字段
         */
        private String OutId;
        
        /**
         * 短信接收日期和时间
         */
        private String ReceiveDate;
        
        /**
         * 短信发送日期和时间
         */
        private String SendDate;
        
        /**
         * 接收短信的手机号码
         */
        private String PhoneNum;
        
        /**
         * 短信内容
         */
        private String Content;
        
        /**
         * 短信发送状态，包括：
         * <p>
         * 1：等待回执
         * 2：发送失败
         * 3：发送成功
         */
        private Integer SendStatus;
    }
}
