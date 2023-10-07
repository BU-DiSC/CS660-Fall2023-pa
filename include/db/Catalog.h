#ifndef DB_CATALOG_H
#define DB_CATALOG_H

#include <db/TupleDesc.h>
#include <db/DbFile.h>
#include <db/Utility.h>

#include <utility>

namespace db {

    struct Table {
        DbFile *file;
        std::string name;
        std::string pkeyField;
        Table() : file(nullptr), name(""), pkeyField("") {} // 添加默认构造函数
        Table(DbFile *file, const std::string &name, const std::string &pkeyField) : file(file), name(name), pkeyField(pkeyField) {}
    };

    /**
     * The Catalog keeps track of all available tables in the database and their
     * associated schemas.
     * For now, this is a stub catalog that must be populated with tables by a
     * user program before it can be used -- eventually, this should be converted
     * to a catalog that reads a catalog table from disk.
     */
    class Catalog {
        // TODO pa1.2: add private members
        std::unordered_map<std::string, Table> tables;
    public:
        // disable copy
        Catalog(const Catalog &) = delete;

        /**
         * Constructor.
         * Creates a new, empty catalog.
         */
        Catalog() = default;

        /**
         * Add a new table to the catalog.
         * This table's contents are stored in the specified DbFile.
         * @param file the contents of the table to add;  file.getId() is the identifier of
         *    this file/tupledesc param for the calls getTupleDesc and getFile
         * @param name the name of the table -- may be an empty string.  May not be null.  If a name
         * conflict exists, use the last table to be added as the table for a given name.
         * @param pkeyField the name of the primary key field
         */
        void addTable(DbFile *file, const std::string &name, const std::string &pkeyField);

        void addTable(DbFile *file, const std::string &name) { addTable(file, name, ""); }

        /**
         * Add a new table to the catalog.
         * This table has tuples formatted using the specified TupleDesc and its
         * contents are stored in the specified DbFile.
         * @param file the contents of the table to add;  file.getId() is the identfier of
         *    this file/tupledesc param for the calls getTupleDesc and getFile
         */
        void addTable(DbFile *file) { addTable(file, Utility::generateUUID(), ""); }

        /**
         * Return the id of the table with a specified name,
         */
        int getTableId(const std::string &name) const;

        /**
         * Returns the tuple descriptor (schema) of the specified table
         * @param tableid The id of the table, as specified by the DbFile.getId()
         *     function passed to addTable
         */
        const TupleDesc &getTupleDesc(int tableid) const;

        /**
         * Returns the DbFile that can be used to read the contents of the
         * specified table.
         * @param tableid The id of the table, as specified by the DbFile.getId()
         *     function passed to addTable
         */
        DbFile *getDatabaseFile(int tableid) const;

        std::string getPrimaryKey(int tableid) const;

        std::string getTableName(int id) const;

        /** Delete all tables from the catalog */
        void clear();
    };
}

#endif
