package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoAggrMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;

public class ConfigInfoAggrMapperByOracle extends OracleAbstractMapper implements ConfigInfoAggrMapper {

	@Override
	public MapperResult batchRemoveAggr(MapperContext context) {
		final List<String> datumList = (List<String>) context.getWhereParameter(FieldConstant.DATUM_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String tenantTmp = (String) context.getWhereParameter(FieldConstant.TENANT_ID);

		List<Object> paramList = new ArrayList<>();
		paramList.add(dataId);
		paramList.add(group);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantTmp)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantTmp);
		}

		final StringBuilder placeholderString = new StringBuilder();
		for (int i = 0; i < datumList.size(); i++) {
			if (i != 0) {
				placeholderString.append(", ");
			}
			placeholderString.append('?');
			paramList.add(datumList.get(i));
		}

		String sql =
				"DELETE FROM config_info_aggr WHERE data_id = ? AND group_id = ? AND tenant_id " + tenantIdQuery + " AND datum_id IN ("
						+ placeholderString + ")";

		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult aggrConfigInfoCount(MapperContext context) {
		final List<String> datumIds = (List<String>) context.getWhereParameter(FieldConstant.DATUM_ID);
		final Boolean isIn = (Boolean) context.getWhereParameter(FieldConstant.IS_IN);
		String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		String tenantTmp = (String) context.getWhereParameter(FieldConstant.TENANT_ID);

		List<Object> paramList = CollectionUtils.list(dataId, group);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantTmp)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantTmp);
		}

		StringBuilder sql = new StringBuilder(
				"SELECT count(*) FROM config_info_aggr WHERE data_id = ? AND group_id = ? AND tenant_id " + tenantIdQuery + " AND datum_id");
		if (isIn) {
			sql.append(" IN (");
		} else {
			sql.append(" NOT IN (");
		}
		for (int i = 0; i < datumIds.size(); i++) {
			if (i > 0) {
				sql.append(", ");
			}
			sql.append('?');
			paramList.add(datumIds.get(i));
		}
		sql.append(')');

		return new MapperResult(sql.toString(), paramList);
	}

	@Override
	public MapperResult findConfigInfoAggrIsOrdered(MapperContext context) {

		String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		String groupId = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		List<Object> paramList = CollectionUtils.list(dataId, groupId);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		String sql = "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM "
				+ "config_info_aggr WHERE data_id = ? AND group_id = ? AND tenant_id " + tenantIdQuery + " ORDER BY datum_id";


		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult findConfigInfoAggrByPageFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		String groupId = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		List<Object> paramList = CollectionUtils.list(dataId, groupId);
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		String sql = "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM config_info_aggr WHERE data_id= ? AND "
				+ "group_id= ? AND tenant_id " + tenantIdQuery + " ORDER BY datum_id";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult findAllAggrGroupByDistinct(MapperContext context) {
		return ConfigInfoAggrMapper.super.findAllAggrGroupByDistinct(context);
	}

	@Override
	public String getTableName() {
		return ConfigInfoAggrMapper.super.getTableName();
	}
}
