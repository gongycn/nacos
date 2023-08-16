package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.GroupCapacityMapper;

public class GroupCapacityMapperByOracle extends OracleAbstractMapper implements GroupCapacityMapper {


	@Override
	public String insertIntoSelectByWhere() {
		return "INSERT INTO group_capacity (group_id, quota,usage, max_size, max_aggr_count, max_aggr_size, gmt_create,"
				+ " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info WHERE group_id=? AND tenant_id is null";
	}


	@Override
	public String updateUsageByWhere() {
		return "UPDATE group_capacity SET usage = (SELECT count(*) FROM config_info WHERE group_id=? AND tenant_id is null),"
				+ " gmt_modified = ? WHERE group_id= ?";
	}

	@Override
	public String selectGroupInfoBySize() {
		return " SELECT id, group_id FROM group_capacity WHERE id > ? and ROWNUM <= ? ";
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

}
