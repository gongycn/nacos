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

}
