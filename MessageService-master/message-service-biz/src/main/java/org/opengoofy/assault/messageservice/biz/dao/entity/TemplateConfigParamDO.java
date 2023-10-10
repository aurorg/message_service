package org.opengoofy.assault.messageservice.biz.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import org.opengoofy.assault.framework.starter.database.BaseDO;

/**
 * 消息模板参数配置实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@TableName("template_config_param")
public class TemplateConfigParamDO extends BaseDO {
    
    /**
     * id
     */
    private Long id;
    
    /**
     * 模板id
     */
    private String templateId;
    
    /**
     * 占位符key
     */
    private String keyPlaceholder;
    
    /**
     * 占位符序号
     */
    private Integer rankPlaceholder;
    
    /**
     * 颜色
     */
    private String colour;
    
    /**
     * 字体大小
     */
    private String size;
}
