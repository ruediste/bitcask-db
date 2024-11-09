#include "bitcask-db.hpp"
#include "test.hpp"

TEST(OpenDB, HappyFlow)
{
    bitcask::BitcaskDb db;
    db.open(createTestDataDir());

    db.put("foo1", "bar1");
    db.put("foo", "bar22");

    ASSERT_EQ(db.getString("foo"), "bar22");
    ASSERT_EQ(db.getString("foo1"), "bar1");
    db.close();
}

TEST(OpenDB, TruncateDb)
{
    auto dir = createTestDataDir();
    bitcask::BitcaskDb db;
    db.open(dir);

    db.put("foo", "bar");

    // read size of db file
    struct stat st;
    stat((dir / "current.log").c_str(), &st);
    auto origSize = st.st_size;

    db.put("foo1", "bar1");
    ASSERT_EQ(db.getString("foo"), "bar");
    ASSERT_EQ(db.getString("foo1"), "bar1");
    db.close();

    // read size again
    stat((dir / "current.log").c_str(), &st);
    auto size = st.st_size;

    while (size > origSize)
    {
        // truncate the db file
        truncate((dir / "current.log").c_str(), --size);
        db.open(dir);
        ASSERT_EQ(db.getString("foo"), "bar");
        std::string result;
        ASSERT_FALSE(db.get("foo1", result));
        db.close();
    }
}
TEST(OpenDB, RotateCurrentLog)
{
    auto dir = createTestDataDir();
    bitcask::BitcaskDb db;
    db.open(dir);

    db.put("foo", "bar");

    db.rotateCurrentLogFile();

    // ASSERT_EQ(db.getString("foo"), "bar");
    db.put("foo1", "bar1");
    // ASSERT_EQ(db.getString("foo"), "bar");
    ASSERT_EQ(db.getString("foo1"), "bar1");
    db.close();

    db = bitcask::BitcaskDb();
    db.open(dir);
    // ASSERT_EQ(db.getString("foo"), "bar");
    // ASSERT_EQ(db.getString("foo1"), "bar1");
    db.close();
}
