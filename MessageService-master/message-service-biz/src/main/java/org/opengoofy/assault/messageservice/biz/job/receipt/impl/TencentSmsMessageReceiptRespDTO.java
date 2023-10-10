package org.opengoofy.assault.messageservice.biz.job.receipt.impl;

import lombok.Data;

/**
 * 腾讯短信回执拉取返回实体
 */
@Data
public class TencentSmsMessageReceiptRespDTO {
    
    /**
     * 唯一请求 ID，每次请求都会返回。定位问题时需要提供该次请求的 RequestId
     */
    private String RequestId;
    
    /**
     * 下发状态响应集合
     */
    private PullSmsSendStatus PullSmsSendStatusSet;
    
    @Data
    public static class PullSmsSendStatus {
        
        /**
         * 用户实际接收到短信的时间，UNIX 时间戳（单位：秒）
         */
        private Integer UserReceiveTime;
        
        /**
         * 国家（或地区）码
         */
        private String CountryCode;
        
        /**
         * 用户号码，普通格式，示例如：13711112222
         */
        private String SubscriberNumber;
        
        /**
         * 手机号码，E.164标准，+[国家或地区码][手机号] ，示例如：+8613711112222， 其中前面有一个+号 ，86为国家码，13711112222为手机号
         */
        private String PhoneNumber;
        
        /**
         * 本次发送标识 ID
         */
        private String SerialNo;
        
        /**
         * 实际是否收到短信接收状态，SUCCESS（成功）、FAIL（失败）
         */
        private String ReportStatus;
        
        /**
         * 用户接收短信状态描述
         */
        private String Description;
    }
}
