#include <db/TupleDesc.h>

using namespace db;

//
// TDItem
//

bool TDItem::operator==(const TDItem &other) const {
    // TODO pa1.1: implement
    return fieldType == other.fieldType && fieldName == other.fieldName;
}

std::size_t std::hash<TDItem>::operator()(const TDItem &r) const {
    // TODO pa1.1: implement
    return std::hash<std::string>()(r.fieldName) ^ std::hash<int>()(static_cast<int>(r.fieldType));
}

//
// TupleDesc
//

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    size = 0;
    for (const auto &type : types) {
        size += Types::getLen(type);
        items.emplace_back(TDItem{type, ""});  // assuming the name is optional and can be an empty string
    }
}

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    if (types.size() != names.size()) {
        throw std::runtime_error("Mismatch in size of types and names vectors");
    }

    size = 0;
    for (size_t i = 0; i < types.size(); i++) {
        size += Types::getLen(types[i]);
        items.emplace_back(TDItem{types[i], names[i]});
    }
}

size_t TupleDesc::numFields() const {
    // TODO pa1.1: implement
    return items.size();
}

std::string TupleDesc::getFieldName(size_t i) const {
    // TODO pa1.1: implement
    if (i >= items.size()) {
        throw std::out_of_range("Index out of range for getFieldNames");
    }
    return items[i].fieldName;
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    // TODO pa1.1: implement
    if (i >= items.size()) {
        throw std::out_of_range("Index out of range for getFieldType");
    }
    return items[i].fieldType;
}

int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    // TODO pa1.1: implement
    for (size_t i = 0; i < items.size(); i++) {
        if (items[i].fieldName == fieldName) {
            return static_cast<int>(i);
        }
    }
    throw std::invalid_argument("Field name not found: " + fieldName);
}

size_t TupleDesc::getSize() const {
    // TODO pa1.1: implement
    return size;
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // TODO pa1.1: implement
    std::vector<Types::Type> mergedTypes;
    std::vector<std::string> mergedNames;

    for (const auto& item : td1.items) {
        mergedTypes.push_back(item.fieldType);
        mergedNames.push_back(item.fieldName);
    }

    for (const auto& item : td2.items) {
        mergedTypes.push_back(item.fieldType);
        mergedNames.push_back(item.fieldName);
    }

    return TupleDesc(mergedTypes, mergedNames);
}

std::string TupleDesc::to_string() const {
    // TODO pa1.1: implement
    std::string result;
    for (const auto& item : items) {
        if (!result.empty()) {
            result += ", ";
        }
        result += item.to_string();
    }
    return result;
}

bool TupleDesc::operator==(const TupleDesc &other) const {
    // TODO pa1.1: implement
    if (items.size() != other.items.size()) {
        return false;
    }

    return std::equal(items.begin(), items.end(), other.items.begin());
}

TupleDesc::iterator TupleDesc::begin() const {
    // TODO pa1.1: implement
    return items.begin();
}

TupleDesc::iterator TupleDesc::end() const {
    // TODO pa1.1: implement
    return items.end();
}

std::size_t std::hash<db::TupleDesc>::operator()(const db::TupleDesc &td) const {
    // TODO pa1.1: implement
    std::size_t seed = 0;
    for (const auto &item : td.items) {
        seed ^= std::hash<db::TDItem>()(item) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}
