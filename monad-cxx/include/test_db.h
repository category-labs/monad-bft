#ifdef __cplusplus
extern "C"
{
#endif

struct TestDb;

struct TestDb *make_testdb();
void testdb_load_callenv(struct TestDb *);
void testdb_load_callcontract(struct TestDb *);
void testdb_load_transfer(struct TestDb *);
char const *testdb_path(struct TestDb const *);
void destroy_testdb(struct TestDb *);

#ifdef __cplusplus
}
#endif
