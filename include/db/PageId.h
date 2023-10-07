#ifndef DB_PAGEID_H
#define DB_PAGEID_H

#include <stdexcept>

namespace db {
    /**
     * PageId is an interface to a specific page of a specific table.
     */
    class PageId {
    public:
        virtual ~PageId() = default;

        virtual int getTableId() const = 0;

        virtual int pageNumber() const = 0;

        virtual bool operator==(const PageId &other) const = 0;

        bool operator!=(const PageId &other) const { return !(*this == other); }
    };
}

template<>
struct std::hash<db::PageId> {
    std::size_t operator()(const db::PageId &r) const {
        return r.getTableId() ^ r.pageNumber();
        // return std::hash<int>()(r.getTableId()) ^ std::hash<int>()(r.pageNumber());
    }
};

#endif
