#ifndef DB_BUFFERPOOL_H
#define DB_BUFFERPOOL_H


#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <stdexcept>
#include <db/PageId.h>
#include <db/Catalog.h>
#include <db/TransactionId.h>
#include <db/Page.h>
#include <db/Tuple.h>

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
namespace db {
    class BufferPool {
        /** Default page size. Use the pageSize member instead. */
        static constexpr int PAGE_SIZE = 4096;
        /** Bytes per page, including header. */
        int pageSize = PAGE_SIZE;
        // TODO pa1.3: add private members
        std::vector<Page*> buffer;
        std::map<PageId*, size_t> pageMap;
        int maxPages;
    public:
        BufferPool(const BufferPool &) = delete;

        /**
         * Default number of pages passed to the constructor.
         * This is used by other classes.
         * BufferPool should use the numPages argument to the constructor instead.
         */
        static constexpr int DEFAULT_PAGES = 50;

        /**
         * Creates a BufferPool that caches up to numPages pages.
         * @param numPages maximum number of pages in this buffer pool.
         */
        explicit BufferPool(int numPages);

        /**
         * Retrieve the specified page.
         * Will acquire a lock and may block if that lock is held by another
         * transaction.
         * <p>
         * The retrieved page should be looked up in the buffer pool.  If it
         * is present, it should be returned.  If it is not present, it should
         * be added to the buffer pool and returned.  If there is insufficient
         * space in the buffer pool, an page should be evicted and the new page
         * should be added in its place.
         *
         * @param tid the ID of the transaction requesting the page
         * @param pid the ID of the requested page
         */
        Page *getPage(const TransactionId &tid, PageId *pid);

        int getPageSize() const { return pageSize; }

        /** DO NOT USE */
        void setPageSize(int newPageSize) { pageSize = newPageSize; }

        /** DO NOT USE */
        void resetPageSize() { setPageSize(PAGE_SIZE); }

        void evictPage();
    };
}

#endif
