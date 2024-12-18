package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.GroupCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;

public class GroupCapacityMapperByOracle extends AbstractMapperByOracle implements GroupCapacityMapper {


	@Override
	public MapperResult selectGroupInfoBySize(MapperContext context) {
		String sql = " SELECT id, group_id FROM group_capacity WHERE id > ? and ROWNUM <= ? ";
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID), context.getPageSize()));
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	@Override
	public MapperResult insertIntoSelectByWhere(MapperContext context) {
		String sql = "INSERT INTO group_capacity (group_id, quota,usage, max_size, max_aggr_count, max_aggr_size, gmt_create,"
				+ " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info WHERE group_id=? AND tenant_id is null";
		List<Object> paramList = new ArrayList<>();
		paramList.add(context.getUpdateParameter(FieldConstant.GROUP_ID));
		paramList.add(context.getUpdateParameter(FieldConstant.QUOTA));
		paramList.add(context.getUpdateParameter(FieldConstant.MAX_SIZE));
		paramList.add(context.getUpdateParameter(FieldConstant.MAX_AGGR_COUNT));
		paramList.add(context.getUpdateParameter(FieldConstant.MAX_AGGR_SIZE));
		paramList.add(context.getUpdateParameter(FieldConstant.GMT_CREATE));
		paramList.add(context.getUpdateParameter(FieldConstant.GMT_MODIFIED));

		paramList.add(context.getWhereParameter(FieldConstant.GROUP_ID));

		return new MapperResult(sql, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	@Override
	public MapperResult updateUsageByWhere(MapperContext context) {
		String sql = "UPDATE group_capacity SET usage = (SELECT count(*) FROM config_info WHERE group_id=? AND tenant_id is null),"
				+ " gmt_modified = ? WHERE group_id= ?";
		return new MapperResult(sql,
				CollectionUtils.list(context.getWhereParameter(FieldConstant.GROUP_ID),
						context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
						context.getWhereParameter(FieldConstant.GROUP_ID)));
	}
}
