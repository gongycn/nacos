package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.HistoryConfigInfoMapper;

public class HistoryConfigInfoMapperByOracle extends OracleAbstractMapper implements HistoryConfigInfoMapper {

	@Override
	public String removeConfigHistory() {
		return "DELETE FROM his_config_info WHERE gmt_modified < ? AND ROWNUM <= ?";
	}

	@Override
	public String findConfigHistoryCountByTime() {
		return "SELECT count(*) FROM his_config_info WHERE gmt_modified < ?";
	}

	@Override
	public String pageFindConfigHistoryFetchRows(int pageNo, int pageSize) {
		final int startRow = (pageNo - 1) * pageSize;
		String sql = "SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM his_config_info "
				+ "WHERE data_id = ? AND group_id = ? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ORDER BY nid DESC";
		return buildPaginationSql(sql, startRow, pageSize);

	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String findConfigHistoryFetchRows() {
		return  "SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM his_config_info "
				+ "WHERE data_id = ? AND group_id = ? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ORDER BY nid DESC";
	}

}
