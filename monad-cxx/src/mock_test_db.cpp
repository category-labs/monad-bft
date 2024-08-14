#include "test_db.hpp"

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

std::string testdb_path(TestDb const *const db)
{
    return std::string{};
}

void destroy_testdb(TestDb *const db) {}
