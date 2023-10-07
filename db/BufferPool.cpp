#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

// TODO pa1.3: implement
BufferPool::BufferPool(int numPages) {
    buffer.reserve(numPages);
    maxPages = numPages;
}

Page *BufferPool::getPage(const TransactionId &tid, PageId *pid) {
    // TODO pa1.3: implement
    if (pageMap.find(pid) != pageMap.end()) {
        size_t pageIndex = pageMap[pid];
        return buffer[pageIndex];
    }

    // If the buffer is full, evict a page to make space
    if (buffer.size() >= static_cast<size_t>(maxPages)) {
        evictPage();
    }

    // TODO: Implement the logic to read the page from your database system
    // Example: Page *page = YourDatabaseAPI::readPage(tid, pid);
    Page *page = nullptr; // Declare 'page' variable here

    // Add the page to the buffer pool and update the page map
    buffer.push_back(page);
    pageMap[pid] = buffer.size() - 1;

    return page;
}

void BufferPool::evictPage() {
    // do nothing for now
}
