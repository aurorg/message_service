package org.opengoofy.assault.messageservice.biz.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import org.opengoofy.assault.framework.starter.database.BaseDO;

/**
 * 消息模板配置实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@TableName("template_config")
public class TemplateConfigDO extends BaseDO {
    
    /**
     * id
     */
    private Long id;
    
    /**
     * 模板 id
     */
    private String templateId;
    
    /**
     * 模板名称
     */
    private String templateName;
    
    /**
     * 三方渠道模板id
     */
    private String channelTemplateId;
    
    /**
     * sid
     */
    private String sourceId;
    
    /**
     * 自定义三方平台渠道id
     */
    private String channelIds;
    
    /**
     * 消息类型 0：短信 1：微信模板消息 2：邮箱 3...
     */
    private Integer msgType;
    
    /**
     * 启用状态 0：启用 1：停用
     */
    private Integer enableStatus;
    
    /**
     * 模板内容
     */
    private String templateText;
}
