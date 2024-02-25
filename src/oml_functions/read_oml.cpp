#include "duckdb/function/table/read_csv.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/table_function/global_csv_state.hpp"
#include "duckdb/execution/operator/csv_scanner/util/csv_error.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/csv_scanner/table_function/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/scanner/string_value_scanner.hpp"

#include <limits>


namespace duckdb {

unique_ptr<CSVFileHandle> ReadCSV::OpenCSV(const string &file_path, FileCompressionType compression,
                                           ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &allocator = BufferAllocator::Get(context);
	return CSVFileHandle::OpenFile(fs, allocator, file_path, compression);
}



void ReadCSVData::FinalizeRead(ClientContext &context) {
	BaseCSVData::Finalize();
	if (!options.rejects_recovery_columns.empty()) {
		for (auto &recovery_col : options.rejects_recovery_columns) {
			bool found = false;
			for (idx_t col_idx = 0; col_idx < return_names.size(); col_idx++) {
				if (StringUtil::CIEquals(return_names[col_idx], recovery_col)) {
					options.rejects_recovery_column_ids.push_back(col_idx);
					found = true;
					break;
				}
			}
			if (!found) {
				throw BinderException("Unsupported parameter for REJECTS_RECOVERY_COLUMNS: column \"%s\" not found",
				                      recovery_col);
			}
		}
	}
}

static unique_ptr<FunctionData> ReadOMLBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ReadCSVData>();
	auto &options = result->options;

    // Changed name to be OML instead of CSV files
	result->files = MultiFileReader::GetFileList(context, input.inputs[0], "OML");





	options.FromNamedParameters(input.named_parameters, context, return_types, names);

	// Validate rejects_table options
	if (!options.rejects_table_name.empty()) {
		if (!options.ignore_errors) {
			throw BinderException("REJECTS_TABLE option is only supported when IGNORE_ERRORS is set to true");
		}
		if (options.file_options.union_by_name) {
			throw BinderException("REJECTS_TABLE option is not supported when UNION_BY_NAME is set to true");
		}
	}

	if (options.rejects_limit != 0) {
		if (options.rejects_table_name.empty()) {
			throw BinderException("REJECTS_LIMIT option is only supported when REJECTS_TABLE is set to a table name");
		}
	}

	if (!options.rejects_recovery_columns.empty() && options.rejects_table_name.empty()) {
		throw BinderException(
		    "REJECTS_RECOVERY_COLUMNS option is only supported when REJECTS_TABLE is set to a table name");
	}

	options.file_options.AutoDetectHivePartitioning(result->files, context);

	if (!options.auto_detect && return_types.empty()) {
		throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
		                      "read_csv_auto or set read_csv(..., "
		                      "AUTO_DETECT=TRUE) to automatically guess columns.");
	}
	if (options.auto_detect && !options.file_options.union_by_name) {
		options.file_path = result->files[0];
		result->buffer_manager = make_shared<CSVBufferManager>(context, options, result->files[0], 0);
		CSVSniffer sniffer(options, result->buffer_manager, CSVStateMachineCache::Get(context),
		                   {&return_types, &names});
		auto sniffer_result = sniffer.SniffCSV();
		if (names.empty()) {
			names = sniffer_result.names;
			return_types = sniffer_result.return_types;
		}
		result->csv_types = return_types;
		result->csv_names = names;
	}

	D_ASSERT(return_types.size() == names.size());
	result->options.dialect_options.num_cols = names.size();
	if (options.file_options.union_by_name) {
		result->reader_bind =
		    MultiFileReader::BindUnionReader<CSVFileScan>(context, return_types, names, *result, options);
		if (result->union_readers.size() > 1) {
			result->column_info.emplace_back(result->initial_reader->names, result->initial_reader->types);
			for (idx_t i = 1; i < result->union_readers.size(); i++) {
				result->column_info.emplace_back(result->union_readers[i]->names, result->union_readers[i]->types);
			}
		}
		if (!options.sql_types_per_column.empty()) {
			auto exception = CSVError::ColumnTypesError(options.sql_types_per_column, names);
			if (!exception.error_message.empty()) {
				throw BinderException(exception.error_message);
			}
			for (idx_t i = 0; i < names.size(); i++) {
				auto it = options.sql_types_per_column.find(names[i]);
				if (it != options.sql_types_per_column.end()) {
					return_types[i] = options.sql_type_list[it->second];
				}
			}
		}
		result->csv_types = return_types;
		result->csv_names = names;
	} else {
		result->csv_types = return_types;
		result->csv_names = names;
		result->reader_bind = MultiFileReader::BindOptions(options.file_options, result->files, return_types, names);
	}
	result->return_types = return_types;
	result->return_names = names;

	result->FinalizeRead(context);
	return std::move(result);
}
}