#ifndef DB_TUPLEDESC_H
#define DB_TUPLEDESC_H

#include <db/Type.h>
#include <string>
#include <stdexcept>
#include <utility>
#include <vector>

namespace db {
    /**
     * A helper class to facilitate organizing the information of each field
     */
    class TDItem {
        int size;
    public:
        /**
         * The type of the field
         */
        Types::Type fieldType;

        /**
         * The name of the field
         */
        std::string fieldName;

        TDItem(Types::Type t, std::string n) : fieldType(t), fieldName(std::move(n)) {};

        bool operator==(const TDItem &other) const;

        bool operator!=(const TDItem &other) const { return !(*this == other); }

        std::string to_string() const { return fieldName + "(" + Types::to_string(fieldType) + ")"; };
    };

    /**
     * TupleDesc describes the schema of a tuple.
     */
    class TupleDesc {
        // TODO pa1.1: add private members
        std::size_t size;
//        using iterator = std::vector<TDItem>::iterator; // replace this with a container iterator or a custom iterator implementation
        std::vector<TDItem> items;
        using iterator = std::vector<TDItem>::const_iterator;
        friend struct std::hash<TupleDesc>;

    public:
        TupleDesc() {}

        /**
         * Create a new TupleDesc with types.length fields with fields of the
         * specified types, with associated named fields.
         *
         * @param types
         *            array specifying the number of and types of fields in this
         *            TupleDesc. It must contain at least one entry.
         * @param names
         *            array specifying the names of the fields.
         */
        TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names);

        /**
         * Constructor. Create a new tuple desc with typeAr.length fields with
         * fields of the specified types, with anonymous (unnamed) fields.
         *
         * @param typeAr
         *            array specifying the number of and types of fields in this
         *            TupleDesc. It must contain at least one entry.
         */
        explicit TupleDesc(const std::vector<Types::Type> &types);

        /**
         * @return the number of fields in this TupleDesc
         */
        size_t numFields() const;

        /**
         * Gets the field name of the ith field of this TupleDesc.
         *
         * @param i
         *            index of the field name to return. It must be a valid index.
         * @return the name of the ith field
         */
        std::string getFieldName(size_t i) const;

        /**
         * Gets the type of the ith field of this TupleDesc.
         *
         * @param i
         *            The index of the field to get the type of. It must be a valid
         *            index.
         * @return the type of the ith field
         */
        Types::Type getFieldType(size_t i) const;

        /**
         * Find the index of the field with a given name.
         *
         * @param name
         *            name of the field.
         * @return the index of the field that is first to have the given name.
         */
        int fieldNameToIndex(const std::string &fieldName) const;

        /**
         * @return The size (in bytes) of tuples corresponding to this TupleDesc.
         *         Note that tuples from a given TupleDesc are of a fixed size.
         */
        size_t getSize() const;

        /**
         * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
         * with the first td1.numFields coming from td1 and the remaining from td2.
         *
         * @param td1
         *            The TupleDesc with the first fields of the new TupleDesc
         * @param td2
         *            The TupleDesc with the last fields of the TupleDesc
         * @return the new TupleDesc
         */
        static TupleDesc merge(const TupleDesc &td1, const TupleDesc &td2);

        /**
         * Compares the specified object with this TupleDesc for equality. Two
         * TupleDescs are considered equal if they are the same size and if the n-th
         * type in this TupleDesc is equal to the n-th type in td.
         *
         * @param o
         *            the Object to be compared for equality with this TupleDesc.
         * @return true if the object is equal to this TupleDesc.
         */
        bool operator==(const TupleDesc &other) const;

        bool operator!=(const TupleDesc &other) const { return !(*this == other); }

        /**
         * Returns a string describing this descriptor. It should be of the form
         * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
         * the exact format does not matter.
         *
         * @return String describing this descriptor.
         */
        std::string to_string() const;

        iterator begin() const;

        iterator end() const;
    };
}

template<>
struct std::hash<db::TDItem> {
    std::size_t operator()(const db::TDItem &o) const;
};

template<>
struct std::hash<db::TupleDesc> {
    std::size_t operator()(const db::TupleDesc &o) const;
};

#endif
