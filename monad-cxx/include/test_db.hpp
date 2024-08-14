#pragma once

#include <string>

struct TestDb;

TestDb *make_testdb();
void testdb_load_callenv(TestDb *);
void testdb_load_callcontract(TestDb *);
void testdb_load_transfer(TestDb *);
std::string testdb_path(TestDb const *);
void destroy_testdb(TestDb *);
