#include <db/Tuple.h>

using namespace db;

//
// Tuple
//

// TODO pa1.1: implement
Tuple::Tuple(const TupleDesc &td, RecordId *rid): desc(td), recordId(rid) {
    fields.resize(td.numFields());
}

const TupleDesc &Tuple::getTupleDesc() const {
    // TODO pa1.1: implement
    return desc;
}

const RecordId *Tuple::getRecordId() const {
    // TODO pa1.1: implement
    return recordId;
}

void Tuple::setRecordId(const RecordId *id) {
    // TODO pa1.1: implement
    recordId = id;
}

const Field &Tuple::getField(int i) const {
    // TODO pa1.1: implement
    if (i >= 0 && i < fields.size() && fields[i]) {
        return *fields[i];
    }
    throw std::runtime_error("Invalid field index or field not set");
}

void Tuple::setField(int i, const Field *f) {
    // TODO pa1.1: implement
    if (i >= 0 && i < fields.size()) {
        fields[i] = const_cast<Field*>(f);
    } else {
        throw std::runtime_error("Invalid field index");
    }
}

// TO BE CONSIDERED
const std::__wrap_iter<std::vector<Field *, std::allocator<Field *>>::const_pointer> Tuple::begin() const {
    // TODO pa1.1: implement
    return fields.cbegin();
}

const std::__wrap_iter<std::vector<Field *, std::allocator<Field *>>::const_pointer> Tuple::end() const {
    // TODO pa1.1: implement
    return fields.cend();
}

std::string Tuple::to_string() const {
    // TODO pa1.1: implement
    std::string result;
    for (const auto &field : fields) {
        if (field) {
            result += field->to_string() + ", ";
        } else {
            result += "null, ";
        }
    }
    if (!result.empty()) {
        // pop last space and
        result.pop_back();
        result.pop_back();
    }
    return result;
}
