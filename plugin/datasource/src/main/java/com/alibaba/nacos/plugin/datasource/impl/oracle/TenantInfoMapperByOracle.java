package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.TenantInfoMapper;

public class TenantInfoMapperByOracle extends OracleAbstractMapper implements TenantInfoMapper {

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

}
