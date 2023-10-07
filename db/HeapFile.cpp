#include <db/HeapFile.h>
#include <db/TupleDesc.h>
#include <db/Page.h>
#include <db/PageId.h>
#include <db/HeapPage.h>
#include <stdexcept>
#include <sys/stat.h>
#include <fcntl.h>

using namespace db;

//
// HeapFile
//

// TODO pa1.5: implement
HeapFile::HeapFile(const char *fname, const TupleDesc &td) : td(td) {
    filename = std::string(fname);
    fileStream.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    if (!fileStream.is_open()) {
        throw std::runtime_error("Failed to open the heap file: " + filename);
    }
}

int HeapFile::getId() const {
    // TODO pa1.5: implement
    // get the absolute file path, get hash code. ID -> unique identity
    std::filesystem::path absPath = std::filesystem::absolute(filename);
    return std::hash<std::string>{}(absPath.string());
}

const TupleDesc &HeapFile::getTupleDesc() const {
    // TODO pa1.5: implement
    return td;
}

Page *HeapFile::readPage(const PageId &pid) {
    // TODO pa1.5: implement
    // calculate the correct offset in the file
    // you need random access to the file in order to read and write pages at arbitrary offsets
    // should not call BufferPool methods when reading a page from disk
    int pageSize = Database::getBufferPool().getPageSize();
    int offset = pid.pageNumber() * pageSize;

    // Check if offset is out of file bounds.
    fileStream.seekg(0, std::ios::end);
    if (offset < 0 || offset >= fileStream.tellg()) {
        throw std::runtime_error("PageId out of bounds");
    }
    // Seek to the correct offset in the file.
    fileStream.seekg(offset, std::ios::beg);

    uint8_t* buffer = new uint8_t[pageSize];
    fileStream.read(reinterpret_cast<char*>(buffer), pageSize);
    if (fileStream.gcount() != pageSize) {
        throw std::runtime_error("Failed to read the full page from the heap file");
    }
    // convert PageId to HeapPageId
    const HeapPageId heapPageId(pid.getTableId(), pid.pageNumber());
    // heapPage(const HeapPageId &id, uint8_t *data)
    // convert char buffer to uint8_t*
    uint8_t* data = reinterpret_cast<uint8_t*>(buffer);
    // create a HeapPage instance
    Page *page = new HeapPage(heapPageId, data);
    return page;
}
