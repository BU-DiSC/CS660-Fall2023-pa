# Programming Assignment 1

## Implementation Guide

### The Database Class

The `Database` class provides access to a collection of static objects that are the global state of the database. In
particular, this includes methods to access the catalog (the list of all the tables in the database) and the buffer
pool (the collection of database file pages currently in memory). We have implemented the Database class for you. You
should take a look at this [file](../include/db/Database.h) as you will need to access these objects.

### Fields and Tuples

Tuples consist of a collection of Field objects, one per field in the Tuple. `Field` is an interface that different data
types (e.g., integer, string) implement. `Tuple` objects are created by the underlying access methods (e.g., heap files,
or B-trees), as described in the next section. Tuples also have a schema, called a tuple descriptor, represented by a
TupleDesc object. This object consists of a collection of `Type` objects, one per field in the tuple, each of which
describes the type of the corresponding field.

### Catalog

The `Catalog` consists of a collection of the tables and schemas of the tables that are currently in the database. You
will need to support the ability to add a new table, as well as get information about a particular table. Associated
with each table is a `TupleDesc` object that allows operators to determine the types and number of fields in a table.

The global catalog is a single instance of `Catalog` that is allocated for the entire process. The global catalog can be
retrieved via the method Database::getCatalog(), and the same goes for the global buffer pool (using Database::
getBufferPool()).

### BufferPool

The `BufferPool` is responsible for caching pages in memory that have been recently read from disk. All operators read
and write pages from various files on disk through the buffer pool. It consists of a fixed number of pages, defined by
the numPages parameter to the BufferPool constructor. In later assignments, you will implement an eviction policy. For
this one, you only need to implement the constructor and the `BufferPool::getPage()` method used by the SeqScan
operator. The `BufferPool` should store up to `numPages` pages. For this assignment, you can assume that no more
than `numPages` request will be made. In the future, you will be required to implement an eviction policy.

The `Database` class provides a static method, `Database::getBufferPool()`, that returns a reference to the single
BufferPool instance for the entire process.

### HeapFile

Access methods provide a way to read or write data from disk that is arranged in a specific way. For this assignment,
you will implement a heap file access method.

A `HeapFile` object is arranged into a set of pages, each of which consists of a fixed number of bytes for storing
tuples, (defined by the constant `BufferPool::PAGE_SIZE`), including a header. In our database, there is one `HeapFile`
object for each table. Each page in a `HeapFile` is arranged as a set of slots, each of which can hold one tuple (tuples
for a given table are all of the same size). In addition to these slots, each page has a header that consists of a
bitmap with one bit per tuple slot. If the bit corresponding to a particular tuple is 1, it indicates that the tuple is
valid; if it is 0, the tuple is invalid (e.g., has been deleted or was never initialized.) Pages of `HeapFile` objects
are of type `HeapPage` which implements the `Page` interface. Pages are stored in the buffer pool but are read and
written by the `HeapFile` class.

We store heap files on disk in more or less the same format they are stored in memory. Each file consists of page data
arranged consecutively on disk. Each page consists of one or more bytes representing the header, followed by the page
size bytes of actual page content. Each tuple requires `tuple_size * 8` bits for its content and 1 bit for the header.
Thus, the number of tuples that can fit on a single page is:

$\text{tuples per page} = \lfloor (\text{page size} * 8) / (\text{tuple size} * 8 + 1) \rfloor$

Where tuple size is the size of a tuple in the page in bytes. The idea here is that each tuple requires one additional
bit of storage in the header. We compute the number of bits in a page (by multiplying page size by 8), and divide this
quantity by the number of bits in a tuple (including this extra header bit) to get the number of tuples per page. The
floor operation rounds down to the nearest integer number of tuples (we don't want to store partial tuples on a page!)

Once we know the number of tuples per page, the number of bytes required to store the header is simply:

$\text{header bytes} = \text{page size} - \text{tuples per page} * \text{tuple size}$

The low (least significant) bits of each byte represents the status of the slots that are earlier in the file. Hence,
the lowest bit of the first byte represents whether or not the first slot in the page is in use. Also, note that the
high-order bits of the last byte may not correspond to a slot that is actually in the file, since the number of slots
may not be a multiple of 8.

Although you will not use them directly now, we ask you to implement `HeapPage::getNumEmptySlots()`
and `HeapPage::isSlotUsed()`. These require pushing around bits in the page header. You may find it helpful to look at
the other methods that have been provided in `HeapPage` to understand the layout of pages.

You will also need to implement an Iterator over the tuples in the page, which may involve an auxiliary class or data
structure.

After you have implemented `HeapPage`, you will write methods for `HeapFile` to calculate the number of pages in a file
and to read a page from the file. You will then be able to fetch tuples from a file stored on disk.

To read a page from disk, you will first need to calculate the correct offset in the file. Hint: you will need random
access to the file in order to read and write pages at arbitrary offsets. You should not call `BufferPool` methods when
reading a page from disk.

You will also need to implement the `HeapFileIterator`, which should iterate through the tuples of each page in
the `HeapFile`. The iterator must use the `BufferPool::getPage()` method to access pages in the `HeapFile`.

### Operators

Operators are responsible for the actual execution of the query plan. They implement the operations of relational
algebra. Operators are iterator based; each operator implements the `DbIterator` interface.

Operators are connected together into a plan by passing lower-level operators into the constructors of higher-level
operators, i.e., by 'chaining them together.

At the top of the plan, the program simply calls iterates through the root operator; this operator then calls iterates
on its children, and so on, until the leaf operators are called. They fetch tuples from disk and pass them up the tree;
tuples propagate up the plan in this way until they are output at the root or combined or rejected by another operator
in the plan.

For this time, you will only need to implement `SeqScan` operator.

## Exercises

Add any private members needed and implement the methods as indicated by the `// TODO pa1.x` comments in the following
files:

### Exercise 1

- TupleDesc ([header](../include/db/TupleDesc.h) & [implementation](../db/TupleDesc.cpp))
- Tuple ([header](../include/db/Tuple.h) & [implementation](../db/Tuple.cpp))

### Exercise 2

Add any private members needed and implement the methods in:

- Catalog ([header](../include/db/Catalog.h) & [implementation](../db/Catalog.cpp))

### Exercise 3

Add any private members needed and implement the methods in:

- BufferPool ([header](../include/db/BufferPool.h) & [implementation](../db/BufferPool.cpp))

### Exercise 4

- HeapPageId ([header](../include/db/HeapPageId.h) & [implementation](../db/HeapPageId.cpp))
- RecordID ([header](../include/db/RecordId.h) & [implementation](../db/RecordId.cpp))
- HeapPage ([header](../include/db/HeapPage.h) & [implementation](../db/HeapPage.cpp))

### Exercise 5

- HeapFile ([header](../include/db/HeapFile.h) & [implementation](../db/HeapFile.cpp))

### Exercise 6

- SeqScan ([header](../include/db/SeqScan.h) & [implementation](../db/SeqScan.cpp))

This operator sequentially scans all of the tuples from the pages of the table specified by the `tableid` in the
constructor. This operator should access tuples through the `DbFile`.
