#ifndef DB_RECORDID_H
#define DB_RECORDID_H

#include <db/HeapPageId.h>
#include <db/PageId.h>
#include <functional>
#include <stdexcept>

namespace db {
    /**
     * A RecordId is a reference to a specific tuple on a specific page of a
     * specific table.
     */
    class RecordId {
        // TODO pa1.4: add private members
        const PageId *pid;
        int tupleno;
    public:
        RecordId() = default;
        /**
         * Creates a new RecordId referring to the specified PageId and tuple
         * number.
         *
         * @param pid
         *            the pageid of the page on which the tuple resides
         * @param tupleno
         *            the tuple number within the page.
         */
        RecordId(const PageId *pid, int tupleno);

        /**
         * @return the tuple number this RecordId references.
         */
        int getTupleno() const;

        /**
         * @return the page id this RecordId references.
         */
        const PageId *getPageId() const;

        /**
         * Two RecordId objects are considered equal if they represent the same tuple.
         * @return True if this and o represent the same tuple
         */
        bool operator==(const RecordId &other) const;

        bool operator!=(const RecordId &other) const { return !(*this == other); }
    };
}

/**
 * You should implement the hashCode() so that two equal RecordId instances
 * (with respect to equals()) have the same hashCode().
 *
 * @return An int that is the same for equal RecordId objects.
 */
template<>
struct std::hash<db::RecordId> {
    std::size_t operator()(const db::RecordId &r) const;
};

#endif
