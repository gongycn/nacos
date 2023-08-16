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

}
