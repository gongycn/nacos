package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoAggrMapper;

public class ConfigInfoAggrMapperByOracle extends OracleAbstractMapper implements ConfigInfoAggrMapper {

	@Override
	public String findConfigInfoAggrByPageFetchRows(int startRow, int pageSize) {
		String sql = "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM config_info_aggr WHERE data_id= ? AND "
				+ "group_id= ? AND tenant_id= ? ORDER BY datum_id";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

}
