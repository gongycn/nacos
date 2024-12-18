package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoBetaMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConfigInfoBetaMapperByOracle extends AbstractMapperByOracle implements ConfigInfoBetaMapper {
	@Override
	public MapperResult updateConfigInfo4BetaCas(MapperContext context) {

		List<Object> paramList = new ArrayList<>();

		paramList.add(context.getUpdateParameter(FieldConstant.CONTENT));
		paramList.add(context.getUpdateParameter(FieldConstant.MD5));
		paramList.add(context.getUpdateParameter(FieldConstant.BETA_IPS));
		paramList.add(context.getUpdateParameter(FieldConstant.SRC_IP));
		paramList.add(context.getUpdateParameter(FieldConstant.SRC_USER));
		paramList.add(context.getUpdateParameter(FieldConstant.APP_NAME));

		paramList.add(context.getWhereParameter(FieldConstant.DATA_ID));
		paramList.add(context.getWhereParameter(FieldConstant.GROUP_ID));
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		paramList.add(context.getWhereParameter(FieldConstant.MD5));

		final String sql = "UPDATE config_info_beta SET content = ?,md5 = ?,beta_ips = ?,src_ip = ?,src_user = ?,gmt_modified = "
				+ getFunction("NOW()")
				+ ",app_name = ? "
				+ "WHERE data_id = ? AND group_id = ? AND tenant_id " + tenantIdQuery + " AND (md5 = ? OR md5 is null OR md5 = '')";

		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult findAllConfigInfoBetaForDumpAllFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = " SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,beta_ips,encrypted_data_key "
				+ " FROM (SELECT rownum ROW_ID, tmp.id from ( SELECT id FROM config_info_beta "
				+ " ORDER BY id ) tmp WHERE rownum <= " + (startRow + pageSize) + ")" + " g, config_info_beta t WHERE g.id = t.id AND g.ROW_ID >" + startRow;
		return new MapperResult(sql, Collections.emptyList());
	}

	@Override
	public String getTableName() {
		return ConfigInfoBetaMapper.super.getTableName();
	}
}
