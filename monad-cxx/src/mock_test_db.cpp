#include "test_db.h"

struct TestDb
{
};

TestDb *make_testdb()
{
    return nullptr;
}

void testdb_load_callenv(TestDb *const db) {}

void testdb_load_callcontract(TestDb *const db) {}

void testdb_load_transfer(TestDb *const db) {}

char const *testdb_path(TestDb const *const db)
{
    return nullptr;
}

void destroy_testdb(TestDb *const db) {}
