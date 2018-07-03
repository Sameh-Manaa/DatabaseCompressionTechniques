#include <string>
#include <core/global_definitions.hpp>
#include <core/base_column.hpp>
#include <core/column_base_typed.hpp>
#include <core/column.hpp>
#include <core/compressed_column.hpp>

/*this is the include for the example compressed column with empty implementation*/
#include <compression/dictionary_compressed_column.hpp>
#include <compression/RunLengthEncoding.hpp>
#include <compression/BitVectorEncoding.hpp>
#include <compression/DictionaryEncoding.hpp>

#include  "unittest.hpp"

using namespace CoGaDB;

int main() {

    std::cout << "Test #1: BitVectorEncoding<int>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<BitVectorEncoding, int>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #2: BitVectorEncoding<float>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<BitVectorEncoding, float>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #3: BitVectorEncoding<string>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<BitVectorEncoding, std::string>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #4: DictionaryEncoding<int>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<DictionaryEncoding, int>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #5: DictionaryEncoding<float>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<DictionaryEncoding, float>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #6: DictionaryEncoding<string>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<DictionaryEncoding, std::string>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #7: RunLengthEncoding<int>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<RunLengthEncoding, int>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #8: RunLengthEncoding<float>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<RunLengthEncoding, float>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

    std::cout << "Test #9: RunLengthEncoding<string>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    if (!unittest<RunLengthEncoding, std::string>()) {
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!\n\n\n" << std::endl;

//    /****** BULK UPDATE TEST ******/
//    {
//        std::cout << "BULK UPDATE TEST..." << std::endl;
//        boost::shared_ptr<Column<int> > uncompressed_col(new Column<int>("int column", INT));
//        boost::shared_ptr<RunLengthEncoding<int> > compressed_col(new RunLengthEncoding<int>("int column", INT));
//        //boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));
//
//
//        std::vector<int> reference_data(100);
//        for (unsigned int i = 0; i < reference_data.size(); i++) {
//            reference_data[i] = get_rand_value<int>();
//        }
//
//        uncompressed_col->insert(reference_data.begin(), reference_data.end());
//        compressed_col->insert(reference_data.begin(), reference_data.end());
//
//        bool result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col)) == *(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//        if (!result) {
//            std::cerr << std::endl << "operator== TEST FAILED!" << std::endl;
//            return false;
//        }
//        PositionListPtr tids(new PositionList());
//        int new_value = rand() % 100;
//        for (unsigned int i = 0; i < 10; i++) {
//            tids->push_back(rand() % uncompressed_col->size());
//        }
//
//        uncompressed_col->update(tids, new_value);
//        compressed_col->update(tids, new_value);
//
//        result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col)) == *(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//        if (!result) {
//            std::cerr << std::endl << "BULK UPDATE TEST FAILED!" << std::endl;
//            return false;
//        }
//        std::cout << "SUCCESS" << std::endl;
//
//    }
//
//    /****** BULK DELETE TEST ******/
//    {
//        std::cout << "BULK DELETE TEST..." << std::endl;
//        boost::shared_ptr<Column<int> > uncompressed_col(new Column<int>("int column", INT));
//        boost::shared_ptr<RunLengthEncoding<int> > compressed_col(new RunLengthEncoding<int>("int column", INT));
//
//        //boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));
//
//        std::vector<int> reference_data(100);
//        for (unsigned int i = 0; i < reference_data.size(); i++) {
//            reference_data[i] = get_rand_value<int>();
//        }
//
//        uncompressed_col->insert(reference_data.begin(), reference_data.end());
//        compressed_col->insert(reference_data.begin(), reference_data.end());
//
//        bool result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col)) == *(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//        if (!result) {
//            std::cerr << std::endl << "operator== TEST FAILED!" << std::endl;
//            return false;
//        }
//
//        PositionListPtr tids(new PositionList());
//
//        for (unsigned int i = 0; i < 10; i++) {
//            tids->push_back(rand() % uncompressed_col->size());
//        }
//
//        compressed_col->remove(tids);
//        //uncompressed_col->remove(tids);
//
//        result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col)) == *(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//        if (!result) {
//            std::cerr << "BULK DELETE TEST FAILED!" << std::endl;
//            return false;
//        }
//        std::cout << "SUCCESS" << std::endl;
//
//    }
 
    return 0;
}