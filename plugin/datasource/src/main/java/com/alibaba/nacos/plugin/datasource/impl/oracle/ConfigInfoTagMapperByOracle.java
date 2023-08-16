package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoTagMapper;

public class ConfigInfoTagMapperByOracle extends OracleAbstractMapper implements ConfigInfoTagMapper {

	@Override
	public String findAllConfigInfoTagForDumpAllFetchRows(int startRow, int pageSize) {
		String pageSql = " SELECT id FROM config_info_tag ORDER BY id ";
		pageSql = buildPaginationSql(pageSql, startRow, pageSize);
		return " SELECT t.id,data_id,group_id,tenant_id,tag_id,app_name,content,md5,gmt_modified "
				+ " FROM ( " + pageSql +" ) " + "g, config_info_tag t  WHERE g.id = t.id  ";
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

}
