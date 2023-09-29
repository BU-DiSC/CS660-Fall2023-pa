#include <db/TupleDesc.h>

using namespace db;

//
// TDItem
//

bool TDItem::operator==(const TDItem &other) const {
    return this->fieldType==other.fieldType && this->fieldName==other.fieldName;
}

std::size_t std::hash<TDItem>::operator()(const TDItem &r) const {
    std::hash<int> intHash;
    std::hash<std::string> stringHash;

    // Combine the hash values using a simple bitwise operation
    std::size_t hashValue = intHash(r.fieldType) ^ (stringHash(r.fieldName) << 1);

    return hashValue;
}

//
// TupleDesc
//

TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    for (size_t i = 0; i < types.size(); ++i) {
        // Create a TDItem and add it to the items_ vector
        items_.emplace_back(TDItem(types[i], ""));
    }
}

TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    for (size_t i = 0; i < types.size(); ++i) {
        // Create a TDItem and add it to the items_ vector
        items_.emplace_back(TDItem(types[i], names[i]));
    }
}

size_t TupleDesc::numFields() const {
    return items_.size();
}

std::string TupleDesc::getFieldName(size_t i) const {
    return items_[i].fieldName;
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    return items_[i].fieldType;
}

int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    for (size_t i = 0; i < items_.size(); ++i) {
        if (items_[i].fieldName==fieldName) return i;
    }
}

size_t TupleDesc::getSize() const {
    size_t totalSize = 0;
    for (const TDItem& item : items_) {
        // Add the size of each field's type to the total size
        totalSize += Types::getLen(item.fieldType);
    }
    return totalSize;
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    std::vector<Types::Type> mergedTypes;
    std::vector<std::string> mergedNames;

    for (size_t i = 0; i < td1.numFields(); ++i) {
        mergedTypes.push_back(td1.getFieldType(i));
        mergedNames.push_back(td1.getFieldName(i));
    }

    for (size_t i = 0; i < td2.numFields(); ++i) {
        mergedTypes.push_back(td2.getFieldType(i));
        mergedNames.push_back(td2.getFieldName(i));
    }

    // Create the merged TupleDesc
    TupleDesc mergedDesc(mergedTypes, mergedNames);
    return mergedDesc;
}


std::string TupleDesc::to_string() const {
    std::string result;

    for (size_t i = 0; i < items_.size(); ++i) {
        if (i > 0) {
            result += ", ";
        }
        result += Types::to_string(items_[i].fieldType) + "(" + items_[i].fieldName + ")";
    }
    return result;
}

bool TupleDesc::operator==(const TupleDesc &other) const {
    if (this->getSize() != other.getSize()) return false;
    for (size_t i = 0; i < this->numFields(); ++i) {
        if (this->getFieldType(i) != other.getFieldType(i)) return false;
    }
    return true;
}

TupleDesc::iterator TupleDesc::begin() const {
    // Error. Could be wrong
    return items_.begin();
}

TupleDesc::iterator TupleDesc::end() const {
    // Error. Could be wrong
    return items_.end();
}

std::size_t std::hash<db::TupleDesc>::operator()(const db::TupleDesc &td) const {
    std::size_t hashValue = 0;

    // Iterate through the TDItem objects in the TupleDesc
    for (const db::TDItem& item : td) {
        // Combine the hash values of TDItem objects
        std::size_t itemHash = hash<db::TDItem>()(item);

        // Update the hash value by combining it with the itemHash
        hashValue ^= itemHash;
    }

    return hashValue;
}
