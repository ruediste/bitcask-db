#include <iostream>
#include "bitcask-db.hpp"
int main(int argc, char *argv[])
{
    std::cout << "Hello World\n";
    bitcask::BitcaskDb db;
    db.open("data");
    db.put("foo", "bar1");
    db.put("foo2", "bar22");

    std::cout << "result: " << db.getString("foo");
    db.close();
}