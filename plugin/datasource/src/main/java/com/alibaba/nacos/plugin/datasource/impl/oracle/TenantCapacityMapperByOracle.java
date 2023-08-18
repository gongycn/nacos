package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.TenantCapacityMapper;

public class TenantCapacityMapperByOracle extends OracleAbstractMapper implements TenantCapacityMapper {

	@Override
	public String getCapacityList4CorrectUsage() {
		return "SELECT id, tenant_id FROM tenant_capacity WHERE id > ? AND  ROWNUM <= ?";
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String incrementUsageWithDefaultQuotaLimit() {
		return "UPDATE tenant_capacity SET usage = usage + 1, gmt_modified = ? WHERE nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') AND usage <"
				+ " ? AND quota = 0";
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String incrementUsageWithQuotaLimit() {
		return "UPDATE tenant_capacity SET usage = usage + 1, gmt_modified = ? WHERE nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') AND usage < "
				+ "quota AND quota != 0";
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String incrementUsage() {
		return "UPDATE tenant_capacity SET usage = usage + 1, gmt_modified = ? WHERE nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "')";
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String decrementUsage() {
		return "UPDATE tenant_capacity SET usage = usage - 1, gmt_modified = ? WHERE nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') AND usage > 0";
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String correctUsage() {
		return "UPDATE tenant_capacity SET usage = (SELECT count(*) FROM config_info WHERE nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "')), "
				+ "gmt_modified = ? WHERE nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "')";
	}


	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String insertTenantCapacity() {
		return "INSERT INTO tenant_capacity (tenant_id, quota, usage, max_size, max_aggr_count, max_aggr_size, "
				+ "gmt_create, gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info WHERE nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "');";
	}

}
