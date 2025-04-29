#pragma once
#include <cstdint>
#include <vector>

namespace rangedb {
// Define the possible sources for the row data (same as before)
enum class RowSource {
    WORKSPACE, // Data from the current, potentially modified state
    SNAPSHOT   // Data from a specific point-in-time capture
};

class TableRow {
private:
    std::vector<std::int8_t> row_data_; // Stores the raw byte representation of the row
    RowSource source_;
    int cur_data_version_;
    TableRow* epoch_row_;

public:
    TableRow() : source_(RowSource::WORKSPACE), cur_data_version_(0) {}

    TableRow(const std::vector<std::int8_t> &data, RowSource source, int version)
        : row_data_(data), source_(source), cur_data_version_(version) {}

    TableRow(std::vector<std::int8_t> &&data, RowSource source, int version)
        : row_data_(std::move(data)), source_(source), cur_data_version_(version) {}

    // --- Getters ---

    /**
     * @brief Gets the source of this row (Workspace or Snapshot).
     * @return The RowSource enum value.
     */
    RowSource getSource() const { return source_; }

    /**
     * @brief Gets the version number of this row.
     * @return The integer version.
     */
    int getVersion() const { return cur_data_version_; }

    /**
     * @brief Gets a constant reference to the raw byte data vector.
     * The caller is responsible for interpreting these bytes according to the table schema.
     * @return Constant reference to the std::vector<std::int8_t>.
     */
    const std::vector<std::int8_t> &getRawData() const { return row_data_; }

    /**
     * @brief Gets the size of the raw data in bytes.
     * @return The number of bytes in the raw data vector.
     */
    size_t getRawDataSize() const { return row_data_.size(); }

    // --- Setters / Mutators ---

    /**
     * @brief Sets the raw data for this row by copying.
     * Note: This typically implies the row's content has changed. You might
     * need to manually update the version afterwards depending on your logic.
     * @param data The new raw byte data to set.
     */
    void setRawData(const std::vector<std::int8_t> &data) {
        row_data_ = data;
        // Consider if version should be updated here or externally
    }

    /**
     * @brief Sets the raw data for this row by moving.
     * Note: This typically implies the row's content has changed. You might
     * need to manually update the version afterwards depending on your logic.
     * @param data The new raw byte data to set (will be moved from).
     */
    void setRawData(std::vector<std::int8_t> &&data) {
        row_data_ = std::move(data);
        // Consider if version should be updated here or externally
    }

    /**
     * @brief Sets the source for this row.
     * @param source The new RowSource.
     */
    void setSource(RowSource source) { source_ = source; }

    /**
     * @brief Sets the version for this row.
     * @param version The new version number.
     */
    void setVersion(int version) { cur_data_version_ = version; }

    // --- Comparison Operators (Based primarily on Version) ---

    bool operator<(const TableRow &other) const { return cur_data_version_ < other.cur_data_version_; }

    bool operator>(const TableRow &other) const { return cur_data_version_ > other.cur_data_version_; }

    bool operator<=(const TableRow &other) const { return cur_data_version_ <= other.cur_data_version_; }

    bool operator>=(const TableRow &other) const { return cur_data_version_ >= other.cur_data_version_; }

    bool operator==(const TableRow &other) const {
        // Primary comparison is version. You might optionally compare
        // source and rawData_ if versions are equal, if needed.
        return cur_data_version_ == other.cur_data_version_ /* && source_ == other.source_ && rawData_ == other.rawData_ */;
    }

    bool operator!=(const TableRow &other) const { return !(*this == other); }

}; // class TableRow

} // namespace rangedb