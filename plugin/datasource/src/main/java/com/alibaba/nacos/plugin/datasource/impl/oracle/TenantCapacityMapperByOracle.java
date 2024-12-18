package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.TenantCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;

public class TenantCapacityMapperByOracle extends AbstractMapperByOracle implements TenantCapacityMapper {

	@Override
	public MapperResult getCapacityList4CorrectUsage(MapperContext context) {
		String sql = "SELECT id, tenant_id FROM tenant_capacity WHERE id>? LIMIT ?";
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID),
				context.getWhereParameter(FieldConstant.LIMIT_SIZE)));
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult incrementUsageWithDefaultQuotaLimit(MapperContext context) {
		List<Object> paramList = CollectionUtils.list(context.getUpdateParameter(FieldConstant.GMT_MODIFIED));
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		paramList.add(context.getWhereParameter(FieldConstant.USAGE));
		String sql = "UPDATE tenant_capacity SET usage = usage + 1, gmt_modified = ? WHERE tenant_id " + tenantIdQuery + " AND usage <"
				+ " ? AND quota = 0";
		return new MapperResult(sql, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult incrementUsageWithQuotaLimit(MapperContext context) {
		List<Object> paramList = CollectionUtils.list(context.getUpdateParameter(FieldConstant.GMT_MODIFIED));
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		String sql = "UPDATE tenant_capacity SET usage = usage + 1, gmt_modified = ? WHERE tenant_id " + tenantIdQuery + " AND usage < "
				+ "quota AND quota != 0";
		return new MapperResult(sql, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult incrementUsage(MapperContext context) {
		List<Object> paramList = CollectionUtils.list(context.getUpdateParameter(FieldConstant.GMT_MODIFIED));
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		String sql = "UPDATE tenant_capacity SET usage = usage + 1, gmt_modified = ? WHERE tenant_id " + tenantIdQuery;
		return new MapperResult(sql, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult decrementUsage(MapperContext context) {
		List<Object> paramList = CollectionUtils.list(context.getUpdateParameter(FieldConstant.GMT_MODIFIED));
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		String sql = "UPDATE tenant_capacity SET usage = usage - 1, gmt_modified = ? WHERE tenant_id " + tenantIdQuery + " AND usage > 0";
		return new MapperResult(sql, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult correctUsage(MapperContext context) {
		List<Object> paramList = new ArrayList<>();
		Object gmtModified = context.getUpdateParameter(FieldConstant.GMT_MODIFIED);
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
			paramList.add(gmtModified);
		} else {
			paramList.add(tenantId);
			paramList.add(gmtModified);
			paramList.add(tenantId);
		}
		String sql = "UPDATE tenant_capacity SET usage = (SELECT count(*) FROM config_info WHERE tenant_id " + tenantIdQuery + "), "
				+ "gmt_modified = ? WHERE tenant_id " + tenantIdQuery;
		return new MapperResult(sql, paramList);
	}


	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult insertTenantCapacity(MapperContext context) {
		List<Object> paramList = new ArrayList<>();
		paramList.add(context.getUpdateParameter(FieldConstant.TENANT_ID));
		paramList.add(context.getUpdateParameter(FieldConstant.QUOTA));
		paramList.add(context.getUpdateParameter(FieldConstant.MAX_SIZE));
		paramList.add(context.getUpdateParameter(FieldConstant.MAX_AGGR_COUNT));
		paramList.add(context.getUpdateParameter(FieldConstant.MAX_AGGR_SIZE));
		paramList.add(context.getUpdateParameter(FieldConstant.GMT_CREATE));
		paramList.add(context.getUpdateParameter(FieldConstant.GMT_MODIFIED));

		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		String sql = "INSERT INTO tenant_capacity (tenant_id, quota, usage, max_size, max_aggr_count, max_aggr_size, "
				+ "gmt_create, gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info WHERE tenant_id" + tenantIdQuery + ";";
		return new MapperResult(sql, paramList);
	}

	@Override
	public String getFunction(String functionName) {
		return null;
	}
}
