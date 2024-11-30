// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cctz/time_zone.h>

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <orc/OrcFile.hh>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/olap_common.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/olap_common.h"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/sargs/Literal.hh"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/column_type_convert.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/table/transactional_hive_reader.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class RuntimeState;
class TFileRangeDesc;
class TFileScanRangeParams;

namespace io {
class FileSystem;
struct IOContext;
} // namespace io
namespace vectorized {
class Block;
template <typename T>
class ColumnVector;
template <typename T>
class DataTypeDecimal;
template <DecimalNativeTypeConcept T>
struct Decimal;
} // namespace vectorized
} // namespace doris
namespace orc {
template <class T>
class DataBuffer;
} // namespace orc

namespace doris::vectorized {

class ORCFileInputStream;

struct LazyReadContext {
    VExprContextSPtrs conjuncts;
    bool can_lazy_read = false;
    // block->rows() returns the number of rows of the first column,
    // so we should check and resize the first column
    bool resize_first_column = true;
    std::list<std::string> all_read_columns;
    // include predicate_partition_columns & predicate_missing_columns
    std::vector<uint32_t> all_predicate_col_ids;
    // save slot_id to find dict filter column name, because expr column name may
    // be different with orc column name
    // std::pair<std::list<col_name>, std::vector<slot_id>>
    std::pair<std::list<std::string>, std::vector<int>> predicate_columns;
    // predicate orc file column names
    std::list<std::string> predicate_orc_columns;
    std::vector<std::string> lazy_read_columns;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            predicate_partition_columns;
    // lazy read partition columns or all partition columns
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> predicate_missing_columns;
    // lazy read missing columns or all missing columns
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
};

class OrcReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(OrcReader);

public:
    struct Statistics {
        int64_t fs_read_time = 0;
        int64_t fs_read_calls = 0;
        int64_t fs_read_bytes = 0;
        int64_t column_read_time = 0;
        int64_t get_batch_time = 0;
        int64_t create_reader_time = 0;
        int64_t init_column_time = 0;
        int64_t set_fill_column_time = 0;
        int64_t decode_value_time = 0;
        int64_t decode_null_map_time = 0;
        int64_t filter_block_time = 0;
    };

    OrcReader(RuntimeProfile* profile, RuntimeState* state, const TFileScanRangeParams& params,
              const TFileRangeDesc& range, size_t batch_size, const std::string& ctz,
              io::IOContext* io_ctx, bool enable_lazy_mat = true,
              std::vector<orc::TypeKind>* unsupported_pushdown_types = nullptr);

    OrcReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
              const std::string& ctz, io::IOContext* io_ctx, bool enable_lazy_mat = true);

    ~OrcReader() override;
    //If you want to read the file by index instead of column name, set hive_use_column_names to false.
    Status init_reader(
            const std::vector<std::string>* column_names,
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
            const VExprContextSPtrs& conjuncts, bool is_acid,
            const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts,
            const bool hive_use_column_names = true);

    Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) override;

    Status _init_select_types(const orc::Type& type, int idx);

    Status _fill_partition_columns(
            Block* block, size_t rows,
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns);
    Status _fill_missing_columns(
            Block* block, size_t rows,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns);

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status get_next_block_impl(Block* block, size_t* read_rows, bool* eof);

    void _fill_batch_vec(std::vector<orc::ColumnVectorBatch*>& result,
                         orc::ColumnVectorBatch* batch, int idx);

    void _build_delete_row_filter(const Block* block, size_t rows);

    int64_t size() const;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<TypeDescriptor>* col_types) override;

    Status get_schema_col_name_attribute(std::vector<std::string>* col_names,
                                         std::vector<uint64_t>* col_attributes,
                                         std::string attribute);
    void set_table_col_to_file_col(
            std::unordered_map<std::string, std::string> table_col_to_file_col) {
        _table_col_to_file_col = table_col_to_file_col;
    }

    void set_position_delete_rowids(vector<int64_t>* delete_rows) {
        _position_delete_ordered_rowids = delete_rows;
    }
    void _execute_filter_position_delete_rowids(IColumn::Filter& filter);

    void set_delete_rows(const TransactionalHiveReader::AcidRowIDSet* delete_rows) {
        _delete_rows = delete_rows;
    }

    Status filter(orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size, void* arg);

    Status fill_dict_filter_column_names(
            std::unique_ptr<orc::StripeInformation> current_strip_information,
            std::list<std::string>& column_names);

    Status on_string_dicts_loaded(
            std::unordered_map<std::string, orc::StringDictionary*>& column_name_to_dict_map,
            bool* is_stripe_filtered);

    static TypeDescriptor convert_to_doris_type(const orc::Type* orc_type);
    static std::string get_field_name_lower_case(const orc::Type* orc_type, int pos);

protected:
    void _collect_profile_before_close() override;

private:
    struct OrcProfile {
        RuntimeProfile::Counter* read_time = nullptr;
        RuntimeProfile::Counter* read_calls = nullptr;
        RuntimeProfile::Counter* read_bytes = nullptr;
        RuntimeProfile::Counter* column_read_time;
        RuntimeProfile::Counter* get_batch_time = nullptr;
        RuntimeProfile::Counter* create_reader_time = nullptr;
        RuntimeProfile::Counter* init_column_time = nullptr;
        RuntimeProfile::Counter* set_fill_column_time = nullptr;
        RuntimeProfile::Counter* decode_value_time = nullptr;
        RuntimeProfile::Counter* decode_null_map_time = nullptr;
        RuntimeProfile::Counter* filter_block_time = nullptr;
        RuntimeProfile::Counter* selected_row_group_count = nullptr;
        RuntimeProfile::Counter* evaluated_row_group_count = nullptr;
    };

    class ORCFilterImpl : public orc::ORCFilter {
    public:
        ORCFilterImpl(OrcReader* orcReader) : _orcReader(orcReader) {}
        ~ORCFilterImpl() override = default;
        void filter(orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size,
                    void* arg) const override {
            if (_status.ok()) {
                _status = _orcReader->filter(data, sel, size, arg);
            }
        }
        Status get_status() { return _status; }

    private:
        mutable Status _status = Status::OK();
        OrcReader* _orcReader = nullptr;
    };

    class StringDictFilterImpl : public orc::StringDictFilter {
    public:
        StringDictFilterImpl(OrcReader* orc_reader) : _orc_reader(orc_reader) {}
        ~StringDictFilterImpl() override = default;

        virtual void fillDictFilterColumnNames(
                std::unique_ptr<orc::StripeInformation> current_strip_information,
                std::list<std::string>& column_names) const override {
            if (_status.ok()) {
                _status = _orc_reader->fill_dict_filter_column_names(
                        std::move(current_strip_information), column_names);
            }
        }
        virtual void onStringDictsLoaded(
                std::unordered_map<std::string, orc::StringDictionary*>& column_name_to_dict_map,
                bool* is_stripe_filtered) const override {
            if (_status.ok()) {
                _status = _orc_reader->on_string_dicts_loaded(column_name_to_dict_map,
                                                              is_stripe_filtered);
            }
        }

        Status get_status() { return _status; }

    private:
        mutable Status _status = Status::OK();
        OrcReader* _orc_reader = nullptr;
    };

    //class RowFilter : public orc::RowReader

    // Create inner orc file,
    // return EOF if file is empty
    // return EROOR if encounter error.
    Status _create_file_reader();

    void _init_profile();
    Status _init_read_columns();
    void _init_orc_cols(const orc::Type& type, std::vector<std::string>& orc_cols,
                        std::vector<std::string>& orc_cols_lower_case,
                        std::unordered_map<std::string, const orc::Type*>& type_map,
                        bool* is_hive1_orc);
    static bool _check_acid_schema(const orc::Type& type);
    static const orc::Type& _remove_acid(const orc::Type& type);

    // functions for building search argument until _init_search_argument
    std::tuple<bool, orc::Literal, orc::PredicateDataType> _make_orc_literal(
            const VSlotRef* slot_ref, const VLiteral* literal);
    bool _check_slot_can_push_down(const VExprSPtr& expr);
    bool _check_literal_can_push_down(const VExprSPtr& expr, uint16_t child_id);
    bool _check_rest_children_can_push_down(const VExprSPtr& expr);
    bool _check_expr_can_push_down(const VExprSPtr& expr);
    void _build_less_than(const VExprSPtr& expr,
                          std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_less_than_equals(const VExprSPtr& expr,
                                 std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_equals(const VExprSPtr& expr, std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_filter_in(const VExprSPtr& expr,
                          std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    void _build_is_null(const VExprSPtr& expr,
                        std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    bool _build_search_argument(const VExprSPtr& expr,
                                std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    bool _init_search_argument(const VExprContextSPtrs& conjuncts);

    void _init_bloom_filter(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);
    void _init_system_properties();
    void _init_file_description();

    template <bool is_filter = false>
    Status _fill_doris_data_column(const std::string& col_name, MutableColumnPtr& data_column,
                                   const DataTypePtr& data_type, const orc::Type* orc_column_type,
                                   orc::ColumnVectorBatch* cvb, size_t num_values);

    template <bool is_filter = false>
    Status _orc_column_to_doris_column(const std::string& col_name, ColumnPtr& doris_column,
                                       const DataTypePtr& data_type,
                                       const orc::Type* orc_column_type,
                                       orc::ColumnVectorBatch* cvb, size_t num_values);

    template <typename CppType, typename OrcColumnType>
    Status _decode_flat_column(const std::string& col_name, const MutableColumnPtr& data_column,
                               orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        OrcColumnType* data = dynamic_cast<OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for column '{}', expected {}", col_name,
                                         cvb->toString());
        }
        auto* cvb_data = data->data.data();
        auto& column_data = static_cast<ColumnVector<CppType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        for (int i = 0; i < num_values; ++i) {
            column_data[origin_size + i] = (CppType)cvb_data[i];
        }
        return Status::OK();
    }

    template <typename DecimalPrimitiveType>
    void _init_decimal_converter(const DataTypePtr& data_type, DecimalScaleParams& scale_params,
                                 const int32_t orc_decimal_scale) {
        if (scale_params.scale_type != DecimalScaleParams::NOT_INIT) {
            return;
        }
        auto* decimal_type = reinterpret_cast<DataTypeDecimal<DecimalPrimitiveType>*>(
                const_cast<IDataType*>(remove_nullable(data_type).get()));
        auto dest_scale = decimal_type->get_scale();
        if (dest_scale > orc_decimal_scale) {
            scale_params.scale_type = DecimalScaleParams::SCALE_UP;
            scale_params.scale_factor = DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(
                    dest_scale - orc_decimal_scale);
        } else if (dest_scale < orc_decimal_scale) {
            scale_params.scale_type = DecimalScaleParams::SCALE_DOWN;
            scale_params.scale_factor = DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(
                    orc_decimal_scale - dest_scale);
        } else {
            scale_params.scale_type = DecimalScaleParams::NO_SCALE;
            scale_params.scale_factor = 1;
        }
    }

    template <typename DecimalPrimitiveType, typename OrcColumnType, bool is_filter>
    Status _decode_explicit_decimal_column(const std::string& col_name,
                                           const MutableColumnPtr& data_column,
                                           const DataTypePtr& data_type,
                                           orc::ColumnVectorBatch* cvb, size_t num_values) {
        OrcColumnType* data = dynamic_cast<OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for column '{}', expected {}", col_name,
                                         cvb->toString());
        }
        if (_decimal_scale_params_index >= _decimal_scale_params.size()) {
            DecimalScaleParams temp_scale_params;
            _init_decimal_converter<DecimalPrimitiveType>(data_type, temp_scale_params,
                                                          data->scale);
            _decimal_scale_params.emplace_back(temp_scale_params);
        }
        DecimalScaleParams& scale_params = _decimal_scale_params[_decimal_scale_params_index];
        ++_decimal_scale_params_index;

        auto* cvb_data = data->values.data();
        auto& column_data =
                static_cast<ColumnDecimal<DecimalPrimitiveType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);

        if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
            for (int i = 0; i < num_values; ++i) {
                int128_t value;
                if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
                    value = static_cast<int128_t>(cvb_data[i]);
                } else {
                    uint64_t hi = data->values[i].getHighBits();
                    uint64_t lo = data->values[i].getLowBits();
                    value = (((int128_t)hi) << 64) | (int128_t)lo;
                }
                value *= scale_params.scale_factor;
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[origin_size + i]);
                v = (DecimalPrimitiveType)value;
            }
        } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
            for (int i = 0; i < num_values; ++i) {
                int128_t value;
                if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
                    value = static_cast<int128_t>(cvb_data[i]);
                } else {
                    uint64_t hi = data->values[i].getHighBits();
                    uint64_t lo = data->values[i].getLowBits();
                    value = (((int128_t)hi) << 64) | (int128_t)lo;
                }
                value /= scale_params.scale_factor;
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[origin_size + i]);
                v = (DecimalPrimitiveType)value;
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                int128_t value;
                if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
                    value = static_cast<int128_t>(cvb_data[i]);
                } else {
                    uint64_t hi = data->values[i].getHighBits();
                    uint64_t lo = data->values[i].getLowBits();
                    value = (((int128_t)hi) << 64) | (int128_t)lo;
                }
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[origin_size + i]);
                v = (DecimalPrimitiveType)value;
            }
        }
        return Status::OK();
    }

    template <bool is_filter>
    Status _decode_int32_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                orc::ColumnVectorBatch* cvb, size_t num_values);

    template <typename DecimalPrimitiveType, bool is_filter>
    Status _decode_decimal_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                  const DataTypePtr& data_type, orc::ColumnVectorBatch* cvb,
                                  size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
            return _decode_explicit_decimal_column<DecimalPrimitiveType, orc::Decimal64VectorBatch,
                                                   is_filter>(col_name, data_column, data_type, cvb,
                                                              num_values);
        } else {
            return _decode_explicit_decimal_column<DecimalPrimitiveType, orc::Decimal128VectorBatch,
                                                   is_filter>(col_name, data_column, data_type, cvb,
                                                              num_values);
        }
    }

    template <typename CppType, typename DorisColumnType, typename OrcColumnType, bool is_filter>
    Status _decode_time_column(const std::string& col_name, const MutableColumnPtr& data_column,
                               orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        auto* data = dynamic_cast<OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for column '{}', expected {}", col_name,
                                         cvb->toString());
        }
        date_day_offset_dict& date_dict = date_day_offset_dict::get();
        auto& column_data = static_cast<ColumnVector<DorisColumnType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        UInt8* __restrict filter_data;
        if constexpr (is_filter) {
            filter_data = _filter->data();
        }
        for (int i = 0; i < num_values; ++i) {
            auto& v = reinterpret_cast<CppType&>(column_data[origin_size + i]);
            if constexpr (std::is_same_v<OrcColumnType, orc::LongVectorBatch>) { // date
                if constexpr (is_filter) {
                    if (!filter_data[i]) {
                        continue;
                    }
                }
                int64_t date_value = data->data[i] + _offset_days;
                if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                    v.create_from_date_v2(date_dict[date_value], TIME_DATE);
                    // we should cast to date if using date v1.
                    v.cast_to_date();
                } else {
                    v = date_dict[date_value];
                }
            } else { // timestamp
                if constexpr (is_filter) {
                    if (!filter_data[i]) {
                        continue;
                    }
                }
                v.from_unixtime(data->data[i], _time_zone);
                if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    // nanoseconds will lose precision. only keep microseconds.
                    v.set_microsecond(data->nanoseconds[i] / 1000);
                }
            }
        }
        return Status::OK();
    }

    template <bool is_filter>
    Status _decode_string_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                 const orc::TypeKind& type_kind, orc::ColumnVectorBatch* cvb,
                                 size_t num_values);

    template <bool is_filter>
    Status _decode_string_non_dict_encoded_column(const std::string& col_name,
                                                  const MutableColumnPtr& data_column,
                                                  const orc::TypeKind& type_kind,
                                                  orc::EncodedStringVectorBatch* cvb,
                                                  size_t num_values);

    template <bool is_filter>
    Status _decode_string_dict_encoded_column(const std::string& col_name,
                                              const MutableColumnPtr& data_column,
                                              const orc::TypeKind& type_kind,
                                              orc::EncodedStringVectorBatch* cvb,
                                              size_t num_values);

    Status _fill_doris_array_offsets(const std::string& col_name,
                                     ColumnArray::Offsets64& doris_offsets,
                                     orc::DataBuffer<int64_t>& orc_offsets, size_t num_values,
                                     size_t* element_size);

    void _collect_profile_on_close();

    bool _can_filter_by_dict(int slot_id);

    Status _rewrite_dict_conjuncts(std::vector<int32_t>& dict_codes, int slot_id, bool is_nullable);

    Status _convert_dict_cols_to_string_cols(Block* block,
                                             const std::vector<orc::ColumnVectorBatch*>* batch_vec);

    MutableColumnPtr _convert_dict_column_to_string_column(const ColumnInt32* dict_column,
                                                           const NullMap* null_map,
                                                           orc::ColumnVectorBatch* cvb,
                                                           const orc::Type* orc_column_typ);
    int64_t get_remaining_rows() { return _remaining_rows; }
    void set_remaining_rows(int64_t rows) { _remaining_rows = rows; }

    // check if the given name is like _col0, _col1, ...
    static bool inline _is_hive1_col_name(const std::string& name) {
        if (name.size() <= 4) {
            return false;
        }
        if (name.substr(0, 4) != "_col") {
            return false;
        }
        for (size_t i = 4; i < name.size(); ++i) {
            if (!isdigit(name[i])) {
                return false;
            }
        }
        return true;
    }

private:
    // This is only for count(*) short circuit read.
    // save the total number of rows in range
    int64_t _remaining_rows = 0;
    RuntimeProfile* _profile = nullptr;
    RuntimeState* _state = nullptr;
    const TFileScanRangeParams& _scan_params;
    const TFileRangeDesc& _scan_range;
    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;
    size_t _batch_size;
    int64_t _range_start_offset;
    int64_t _range_size;
    const std::string& _ctz;
    const std::vector<std::string>* _column_names;
    int32_t _offset_days = 0;
    cctz::time_zone _time_zone;

    std::list<std::string> _read_cols;
    std::list<std::string> _read_cols_lower_case;
    std::list<std::string> _missing_cols;
    std::unordered_map<std::string, int> _colname_to_idx;
    // Column name in Orc file after removed acid(remove row.) to column name to schema.
    // This is used for Hive 1.x which use internal column name in Orc file.
    // _col0, _col1...
    std::unordered_map<std::string, std::string> _removed_acid_file_col_name_to_schema_col;
    // Flag for hive engine.
    // 1. True if the external table engine is Hive1.x with orc col name as _col1, col2, ...
    // 2. If true, use indexes instead of column names when reading orc tables.
    bool _is_hive1_orc_or_use_idx = false;

    std::unordered_map<std::string, std::string> _col_name_to_file_col_name;
    // TODO: check if we can remove _col_name_to_file_col_name_low_case
    std::unordered_map<std::string, std::string> _col_name_to_file_col_name_low_case;
    std::unordered_map<std::string, const orc::Type*> _type_map;
    std::vector<const orc::Type*> _col_orc_type;
    std::unique_ptr<ORCFileInputStream> _file_input_stream;
    Statistics _statistics;
    OrcProfile _orc_profile;
    orc::ReaderMetrics _reader_metrics;

    std::unique_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::Reader> _reader;
    std::unique_ptr<orc::RowReader> _row_reader;
    std::unique_ptr<ORCFilterImpl> _orc_filter;
    orc::RowReaderOptions _row_reader_options;

    std::shared_ptr<io::FileSystem> _file_system;

    io::IOContext* _io_ctx = nullptr;
    bool _enable_lazy_mat = true;
    bool _enable_filter_by_min_max = true;

    std::vector<DecimalScaleParams> _decimal_scale_params;
    size_t _decimal_scale_params_index;

    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    bool _is_acid = false;
    std::unique_ptr<IColumn::Filter> _filter;
    LazyReadContext _lazy_read_ctx;
    const TransactionalHiveReader::AcidRowIDSet* _delete_rows = nullptr;
    std::unique_ptr<IColumn::Filter> _delete_rows_filter_ptr;

    const TupleDescriptor* _tuple_descriptor = nullptr;
    const RowDescriptor* _row_descriptor = nullptr;
    VExprContextSPtrs _not_single_slot_filter_conjuncts;
    const std::unordered_map<int, VExprContextSPtrs>* _slot_id_to_filter_conjuncts = nullptr;
    VExprContextSPtrs _dict_filter_conjuncts;
    VExprContextSPtrs _non_dict_filter_conjuncts;
    VExprContextSPtrs _filter_conjuncts;
    // std::pair<col_name, slot_id>
    std::vector<std::pair<std::string, int>> _dict_filter_cols;
    std::shared_ptr<ObjectPool> _obj_pool;
    std::unique_ptr<StringDictFilterImpl> _string_dict_filter;
    bool _dict_cols_has_converted = false;
    bool _has_complex_type = false;
    std::vector<orc::TypeKind>* _unsupported_pushdown_types;

    // resolve schema change
    std::unordered_map<std::string, std::unique_ptr<converter::ColumnTypeConverter>> _converters;
    //for iceberg table , when table column name != file column name
    //TODO(CXY) : remove _table_col_to_file_col,because we hava _col_name_to_file_col_name，
    // the two have the same effect.
    std::unordered_map<std::string, std::string> _table_col_to_file_col;
    //support iceberg position delete .
    std::vector<int64_t>* _position_delete_ordered_rowids = nullptr;
    std::unordered_map<const VSlotRef*, orc::PredicateDataType>
            _vslot_ref_to_orc_predicate_data_type;
    std::unordered_map<const VLiteral*, orc::Literal> _vliteral_to_orc_literal;
};

class ORCFileInputStream : public orc::InputStream, public ProfileCollector {
public:
    ORCFileInputStream(const std::string& file_name, io::FileReaderSPtr inner_reader,
                       OrcReader::Statistics* statistics, const io::IOContext* io_ctx,
                       RuntimeProfile* profile)
            : _file_name(file_name),
              _inner_reader(inner_reader),
              _file_reader(inner_reader),
              _statistics(statistics),
              _io_ctx(io_ctx),
              _profile(profile) {}

    ~ORCFileInputStream() override {
        if (_file_reader != nullptr) {
            _file_reader->collect_profile_before_close();
        }
    }

    uint64_t getLength() const override { return _file_reader->size(); }

    uint64_t getNaturalReadSize() const override { return config::orc_natural_read_size_mb << 20; }

    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override { return _file_name; }

    void beforeReadStripe(std::unique_ptr<orc::StripeInformation> current_strip_information,
                          std::vector<bool> selected_columns) override;

    void set_all_tiny_stripes() { _is_all_tiny_stripes = true; }

    io::FileReaderSPtr& get_file_reader() { return _file_reader; }

    io::FileReaderSPtr& get_inner_reader() { return _inner_reader; }

protected:
    void _collect_profile_at_runtime() override {};
    void _collect_profile_before_close() override;

private:
    const std::string& _file_name;
    io::FileReaderSPtr _inner_reader;
    io::FileReaderSPtr _file_reader;
    bool _is_all_tiny_stripes = false;
    // Owned by OrcReader
    OrcReader::Statistics* _statistics = nullptr;
    const io::IOContext* _io_ctx = nullptr;
    RuntimeProfile* _profile = nullptr;
};
} // namespace doris::vectorized
