package org.opengoofy.assault.messageservice.biz.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/**
 * 渠道配置实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@TableName("channel_business")
public class ChannelBusinessDO {
    
    /**
     * id
     */
    private Long id;
    
    /**
     * 自定义三方平台渠道id
     */
    private String channelId;
    
    /**
     * Access Key
     */
    private String accessKey;
    
    /**
     * Access Key Secret
     */
    private String accessKeySecret;
    
    /**
     * 扩展信息
     */
    private String extend;
}
