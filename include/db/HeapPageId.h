#ifndef DB_HEAPPAGEID_H
#define DB_HEAPPAGEID_H

#include <db/PageId.h>
#include <functional>
#include <stdexcept>

namespace db {
    /**
     * Unique identifier for HeapPage objects.
     */
    class HeapPageId : public PageId {
        // TODO pa1.4: Add private members
        int tableId;
        int pgNo;
    public:
        /**
         * Constructor. Create a page id structure for a specific page of a
         * specific table.
         *
         * @param tableId The table that is being referenced
         * @param pgNo The page number in that table.
         */
        HeapPageId(int tableId, int pgNo);

        /** @return the table associated with this PageId */
        int getTableId() const override;

        /**
         * @return the page number in the table getTableId() associated with
         *   this PageId
         */
        int pageNumber() const override;

        /**
         * Compares one PageId to another.
         *
         * @param other The object to compare against (must be a PageId)
         * @return true if the objects are equal (e.g., page numbers and table
         *   ids are the same)
         */
        bool operator==(const PageId& other) const override;

        size_t write(void *dest) const {
            int* data = static_cast<int *>(dest);
            data[0] = getTableId();
            data[1] = pageNumber();
            return 2 * sizeof(int);
        };
    };
}

#endif
