package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoBetaMapper;

public class ConfigInfoBetaMapperByOracle extends OracleAbstractMapper implements ConfigInfoBetaMapper {

	@Override
	public String findAllConfigInfoBetaForDumpAllFetchRows(int startRow, int pageSize) {
		return " SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,beta_ips,encrypted_data_key "
				+ " FROM (SELECT rownum ROW_ID, tmp.id from ( SELECT id FROM config_info_beta "
				+ " ORDER BY id ) tmp WHERE rownum <= " + (startRow + pageSize) + ")" + " g, config_info_beta t WHERE g.id = t.id AND g.ROW_ID >" + startRow;
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String updateConfigInfo4BetaCas() {
		return "UPDATE config_info_beta SET content = ?,md5 = ?,beta_ips = ?,src_ip = ?,src_user = ?,gmt_modified = ?,app_name = ? "
				+ "WHERE data_id = ? AND group_id = ? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') AND (md5 = ? OR md5 is null OR md5 = '')";
	}

}
