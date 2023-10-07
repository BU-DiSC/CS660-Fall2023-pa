#include <db/RecordId.h>
#include <stdexcept>

using namespace db;

//
// RecordId
//

// TODO pa1.4: implement
RecordId::RecordId(const PageId *pid, int tupleno): pid(pid), tupleno(tupleno) {
}

bool RecordId::operator==(const RecordId &other) const {
    // TODO pa1.4: implement
    return (*pid == *(other.pid)) && (tupleno == other.tupleno);
}

const PageId *RecordId::getPageId() const {
    // TODO pa1.4: implement
    return pid;
}

int RecordId::getTupleno() const {
    // TODO pa1.4: implement
    return tupleno;
}

//
// RecordId hash function
//

std::size_t std::hash<RecordId>::operator()(const RecordId &r) const {
    // TODO pa1.4: implement
    std::hash<db::PageId> pidContentHash;
    std::hash<int> tupleNoHash;

    return pidContentHash(*(r.getPageId())) ^ (tupleNoHash(r.getTupleno()) << 1);

}
