#include <iostream>
#include "bitcask-db.hpp"
int main(int argc, char *argv[])
{
    std::cout << "Hello World\n";
    bitcask::BitcaskDb db;
    if (!db.open("data"))
    {
        std::cout << "Failed to open db\n";
        return 1;
    }
    // db.put("foo1", "bar1");
    // db.put("foo", "bar22");

    db.dumpIndex();
    std::cout << "result: " << db.getString("foo") << std::endl;
    std::cout << "result: " << db.getString("foo1") << std::endl;
    db.close();
}