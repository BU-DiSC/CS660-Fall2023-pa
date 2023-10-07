#include <db/Catalog.h>
#include <db/TupleDesc.h>
#include <stdexcept>

using namespace db;

void Catalog::addTable(DbFile *file, const std::string &name, const std::string &pkeyField) {
    // TODO pa1.2: implement
    Table table(file, name, pkeyField);
    tables[name] = table;
}

int Catalog::getTableId(const std::string &name) const {
    // TODO pa1.2: implement
    if (tables.find(name) != tables.end()) {
        return tables.at(name).file->getId();
    }
    throw std::invalid_argument("Table not found");

}

const TupleDesc &Catalog::getTupleDesc(int tableid) const {
    // TODO pa1.2: implement
    for (const auto &table : tables) {
        if (table.second.file->getId() == tableid) {
            return table.second.file->getTupleDesc();
        }
    }
    throw std::runtime_error("Table ID not found");
}

DbFile *Catalog::getDatabaseFile(int tableid) const {
    // TODO pa1.2: implement
    for (const auto &table : tables) {
        if (table.second.file->getId() == tableid) {
            return table.second.file;
        }
    }
    throw std::runtime_error("Database file not found");
}

std::string Catalog::getPrimaryKey(int tableid) const {
    // TODO pa1.2: implement
    for (const auto &table : tables) {
        if (table.second.file->getId() == tableid) {
            return table.second.pkeyField;
        }
    }
    throw std::runtime_error("Primary key not found");
}

std::string Catalog::getTableName(int id) const {
    // TODO pa1.2: implement
    for (const auto &table : tables) {
        if (table.second.file->getId() == id) {
            return table.second.name;
        }
    }
    throw std::runtime_error("Table name not found");
}

void Catalog::clear() {
    // TODO pa1.2: implement
    tables.clear();
}