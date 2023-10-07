#include <db/SeqScan.h>

using namespace db;

// TODO pa1.6: implement
SeqScan::SeqScan(TransactionId *tid, int tableid, const std::string &tableAlias)
   : tid(tid), tableid(tableid), tableAlias(tableAlias){
}

std::string SeqScan::getTableName() const {
    // TODO pa1.6: implement
    return Database::getCatalog().getTableName(tableid);
}

std::string SeqScan::getAlias() const {
    // TODO pa1.6: implement
    return tableAlias;
}

void SeqScan::reset(int tabid, const std::string &tableAlias) {
    // TODO pa1.6: implement
    this->tableid = tabid;
    this->tableAlias = tableAlias;
}

const TupleDesc &SeqScan::getTupleDesc() const {
    // TODO pa1.6: implement
    return Database::getCatalog().getTupleDesc(tableid);
}

SeqScan::iterator SeqScan::begin() const {
    // TODO pa1.6: implement
    DbFile *file = Database::getCatalog().getDatabaseFile(tableid);
    return SeqScanIterator(tid, *file, 0);
}

SeqScan::iterator SeqScan::end() const {
    // TODO pa1.6: implement
    return SeqScanIterator(tid, *Database::getCatalog().getDatabaseFile(tableid), -1);
}

//
// SeqScanIterator
//

// TODO pa1.6: implement
//SeqScanIterator::SeqScanIterator(/* TODO pa1.6: add parameters */) {
//}
SeqScanIterator::SeqScanIterator(TransactionId *tid, DbFile &file, int startPageId)
   : transactionId(tid), dbFile(file), currentPageId(startPageId), currentTuplePosition(0) {}

    bool SeqScanIterator::operator!=(const SeqScanIterator &other) const {
    // TODO pa1.6: implement
    return currentPageId != other.currentPageId || currentTuplePosition != other.currentTuplePosition;
}

SeqScanIterator &SeqScanIterator::operator++() {
    // TODO pa1.6: implement
    currentTuplePosition++;
    // Calculate the maximum number of tuples that can fit in a page
    int pageSize = Database::getBufferPool().getPageSize();
    int tupleSize = currentTuple.getTupleDesc().getSize();
    int tuplesPerPage = pageSize / tupleSize;

    // Check if we have reached the end of the current page
    if (currentTuplePosition >= tuplesPerPage) {
        // Move to the next page
        currentPageId++;
        currentTuplePosition = 0;
    }

    return *this;
}

const Tuple &SeqScanIterator::operator*() const {
    // TODO pa1.6: implement
    HeapPageId pid(dbFile.getId(), currentPageId);
    Page *currentPage = Database::getBufferPool().getPage(*transactionId, &pid);
    Tuple* tuples = static_cast<Tuple*>(currentPage->getPageData());
    return tuples[currentTuplePosition];
}

