#include "test_db.h"

struct TestDb
{
};

TestDb *make_testdb()
{
    return nullptr;
}

void testdb_load_callenv(TestDb *const ) {}

void testdb_load_callcontract(TestDb *const ) {}

void testdb_load_transfer(TestDb *const ) {}

char const *testdb_path(TestDb const *const )
{
    return nullptr;
}

void destroy_testdb(TestDb *const ) {}
