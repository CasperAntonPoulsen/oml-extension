#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
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
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/csv_scanner/table_function/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/scanner/string_value_scanner.hpp"

#include <limits>



// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {


static unique_ptr<FunctionData> ReadOMLBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ReadCSVData>();
	auto &options = result->options;

    // Changed name to be OML instead of CSV files
	result->files = MultiFileReader::GetFileList(context, input.inputs[0], "OML");


    vector<string> static_names = {
        "experiment_id",
        "node_id",
        "node_id_seq",
        "time_sec",
        "time_usec",
        "power",
        "current",
        "voltage"
    };
    vector<LogicalType> static_return_types = {
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::UINTEGER,
        LogicalType::UINTEGER,
        LogicalType::DOUBLE,
        LogicalType::DOUBLE,
        LogicalType::DOUBLE
    };

    for (int i = 0; i < static_names.size(); i++) {
        names.push_back(static_names[i]);
        return_types.push_back(static_return_types[i]);
    }

    options.FromNamedParameters(input.named_parameters, context, return_types, names);

    options.SetSkipRows(line_counter);
    options.SetDelimiter("\t");


	options.file_options.AutoDetectHivePartitioning(result->files, context);

	options.file_path = result->files[0];
	result->buffer_manager = make_shared<CSVBufferManager>(context, options, result->files[0], 0);
	result->csv_types = return_types;
	result->csv_names = names;

	D_ASSERT(return_types.size() == names.size());
	result->options.dialect_options.num_cols = names.size();

	result->return_types = return_types;
	result->return_names = names;
	result->reader_bind = MultiFileReader::BindOptions(options.file_options, result->files, return_types, names);
	result->FinalizeRead(context);
	return std::move(result);
}

// This function is based on dbgen.cpp CreateTPCHTable function
static void ReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto read_csv_function = ReadCSVTableFunction::GetFunction();
    auto read_csv_function_function = read_csv_function.function;
    read_csv_function_function(context, data_p, output);
    auto &bind_data = data_p.bind_data->CastNoConst<ReadCSVData>();


    // Create the table if it doesn't exist
    auto info = make_uniq<CreateTableInfo>();

    // Use the databasemanager to get the default database name
    string catalog_name = DatabaseManager::GetDefaultDatabase(context);

    info->catalog = catalog_name;

    // hard code table name
    string table_name = "Power_Consumption";
    info->table = table_name;

    info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    info->temporary = false;

    int column_count = bind_data.csv_names.size();
    for (idx_t i = 0; i < column_count; i++) {
        info->columns.AddColumn(ColumnDefinition(bind_data.csv_names[i],bind_data.csv_types[i]));
        info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
    }

    auto &catalog = Catalog::GetCatalog(context,catalog_name);
    catalog.CreateTable(context, std::move(info));


}






static void LoadInternal(DatabaseInstance &instance) {


    auto read_oml = ReadCSVTableFunction::GetFunction();
    read_oml.name = "read_oml";
    read_oml.bind = ReadOMLBind;
    read_oml.function = ReadOMLFunction;
    
    ExtensionUtil::RegisterFunction(instance, read_oml);
}

void OmlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string OmlExtension::Name() {
	return "oml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::OmlExtension>();
}

DUCKDB_EXTENSION_API const char *oml_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
