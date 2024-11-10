#include <iostream>
#include "bitcask-db.hpp"
#include <gtest/gtest.h>
#include <gtest-internal-inl.h>

#include "test.hpp"

static int nextTestDataNr = 0;
std::filesystem::path testDataDir = "testData";

std::filesystem::path createTestDataDir()
{
    auto result = testDataDir / (std::to_string(nextTestDataNr++));
    std::filesystem::create_directories(result);
    return result;
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // delete test data
    std::filesystem::remove_all(testDataDir);

    return RUN_ALL_TESTS();
}