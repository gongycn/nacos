package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoAggrMapper;

public class ConfigInfoAggrMapperByOracle extends OracleAbstractMapper implements ConfigInfoAggrMapper {

	@Override
	public String findConfigInfoAggrByPageFetchRows(int startRow, int pageSize) {
		String sql = "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM config_info_aggr WHERE data_id= ? AND "
				+ "group_id= ? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ORDER BY datum_id";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}


	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 * @param datumSize the size of datum list
	 * @return
	 */
	public String batchRemoveAggr(int datumSize) {
		final StringBuilder placeholderString = new StringBuilder();
		for (int i = 0; i < datumSize; i++) {
			if (i != 0) {
				placeholderString.append(", ");
			}
			placeholderString.append('?');
		}
		return "DELETE FROM config_info_aggr WHERE data_id = ? AND group_id = ? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') AND datum_id IN ("
				+ placeholderString + ")";
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String aggrConfigInfoCount(int size, boolean isIn) {
		StringBuilder sql = new StringBuilder(
				"SELECT count(*) FROM config_info_aggr WHERE data_id = ? AND group_id = ? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') AND datum_id");
		if (isIn) {
			sql.append(" IN (");
		} else {
			sql.append(" NOT IN (");
		}
		for (int i = 0; i < size; i++) {
			if (i > 0) {
				sql.append(", ");
			}
			sql.append('?');
		}
		sql.append(')');

		return sql.toString();
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String findConfigInfoAggrIsOrdered() {
		return "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM config_info_aggr WHERE data_id = ? AND "
				+ "group_id = ? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ORDER BY datum_id";
	}

}
