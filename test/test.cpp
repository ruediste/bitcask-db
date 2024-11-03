#include <iostream>
#include "bitcask-db.hpp"
int main(int argc, char *argv[])
{
    std::cout << "Hello World\n";
    bitcask::BitcaskDb db;
    db.open("data");
    db.put("foo", "bar1");
    db.put("foo", "bar22");

    db.dumpIndex();
    std::cout << "result: " << db.getString("foo") << std::endl;
    db.close();
}