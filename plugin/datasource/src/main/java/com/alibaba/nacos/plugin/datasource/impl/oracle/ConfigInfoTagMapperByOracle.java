package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoTagMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConfigInfoTagMapperByOracle extends AbstractMapperByOracle implements ConfigInfoTagMapper {

	@Override
	public MapperResult findAllConfigInfoTagForDumpAllFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String pageSql = " SELECT id FROM config_info_tag ORDER BY id ";
		pageSql = buildPaginationSql(pageSql, startRow, pageSize);
		String sql = " SELECT t.id,data_id,group_id,tenant_id,tag_id,app_name,content,md5,gmt_modified "
				+ " FROM ( " + pageSql + " ) " + "g, config_info_tag t  WHERE g.id = t.id  ";
		return new MapperResult(sql, Collections.emptyList());
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult updateConfigInfo4TagCas(MapperContext context) {
		List<Object> paramList = new ArrayList<>();

		Object content = context.getUpdateParameter(FieldConstant.CONTENT);
		paramList.add(content);
		Object md5 = context.getUpdateParameter(FieldConstant.MD5);
		paramList.add(md5);
		Object srcIp = context.getUpdateParameter(FieldConstant.SRC_IP);
		paramList.add(srcIp);
		Object srcUser = context.getUpdateParameter(FieldConstant.SRC_USER);
		paramList.add(srcUser);
		Object gmtModified = context.getUpdateParameter(FieldConstant.GMT_MODIFIED);
		paramList.add(gmtModified);
		Object appName = context.getUpdateParameter(FieldConstant.APP_NAME);
		paramList.add(appName);

		Object dataId = context.getWhereParameter(FieldConstant.DATA_ID);
		paramList.add(dataId);
		Object groupId = context.getWhereParameter(FieldConstant.GROUP_ID);
		paramList.add(groupId);
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		Object tagId = context.getWhereParameter(FieldConstant.TAG_ID);
		paramList.add(tagId);
		Object oldMd5 = context.getWhereParameter(FieldConstant.MD5);
		paramList.add(oldMd5);
		String sql =
				"UPDATE config_info_tag SET content = ?, md5 = ?, src_ip = ?,src_user = ?,gmt_modified = ?,app_name = ? "
						+ "WHERE data_id = ? AND group_id = ? AND tenant_id " + tenantIdQuery + " AND tag_id = ? AND (md5 = ? OR md5 IS NULL)";
		return new MapperResult(sql, paramList);
	}

}
