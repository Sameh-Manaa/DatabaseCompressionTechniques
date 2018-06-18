#pragma once

#include <core/compressed_column.hpp>
#include <utility>
#include <boost/serialization/utility.hpp>
#include <map>
#include <boost/serialization/map.hpp>
#include <boost/dynamic_bitset.hpp>

namespace CoGaDB {

    template<class T>
    class DictionaryEncoding : public CompressedColumn<T> {
    public:
        /***************** constructors and destructor *****************/
        DictionaryEncoding(const std::string& name, AttributeType db_type);
        virtual ~DictionaryEncoding();

        virtual bool insert(const boost::any& new_Value);
        virtual bool insert(const T& new_value);
        template <typename InputIterator>
        bool insert(InputIterator first, InputIterator last);

        virtual bool update(TID tid, const boost::any& new_value);
        virtual bool update(PositionListPtr tid, const boost::any& new_value);

        virtual bool remove(TID tid);
        //assumes tid list is sorted ascending
        virtual bool remove(PositionListPtr tid);
        virtual bool clearContent();

        virtual const boost::any get(TID tid);
        //virtual const boost::any* const getRawData()=0;
        virtual void print() const throw ();
        virtual size_t size() const throw ();
        virtual unsigned int getSizeinBytes() const throw ();

        virtual const ColumnPtr copy() const;

        virtual bool store(const std::string& path);
        virtual bool load(const std::string& path);

        virtual int getNeededBits(uint64_t value);

        virtual std::vector<bool> getNewDictionaryCode();



        virtual T& operator[](const int index);

    private:

        /*compressed values structure*/
        /*map<[VALUE],[DICTIONARY_CODE]>*/
        std::map<T, std::vector<bool> > dictionary;
        /*vector<[DICTIONARY_CODE]>*/
        std::vector<std::vector<bool> > encodedValues;
    };

    /***************** Start of Implementation Section ******************/


    template<class T>
    DictionaryEncoding<T>::DictionaryEncoding(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type) {

    }

    template<class T>
    DictionaryEncoding<T>::~DictionaryEncoding() {

    }

    /*
     * credit: 
     * https://stackoverflow.com/questions/21191307/minimum-number-of-bits-are-to-represent-a-given-int
     */
    template<class T>
    int DictionaryEncoding<T>::getNeededBits(uint64_t value) {
        if (value <= 1) {
            return 1;
        }
        int bits = 0;
        for (int bit_test = 16; bit_test > 0; bit_test >>= 1) {
            if (value >> bit_test != 0) {
                bits += bit_test;
                value >>= bit_test;
            }
        }
        return bits + value;
    }

    template<class T>
    std::vector<bool> DictionaryEncoding<T>::getNewDictionaryCode() {
        boost::dynamic_bitset<> code(this->getNeededBits(dictionary.size()), dictionary.size());

        std::vector<bool> dictionaryCode;

        for (int i = 0; i < code.size(); i++) {
            dictionaryCode.push_back(code[i]);
        }
        return dictionaryCode;
    }

    template<class T>
    bool DictionaryEncoding<T>::insert(const boost::any& newValue) {
        //check for different type value
        if (typeid (T) == newValue.type()) {
            return false;
        }

        T value = boost::any_cast<T>(newValue);

        //insert the value along with its dictionaryCode into the dictionary map(if not already there)
        dictionary.insert(std::make_pair(value, getNewDictionaryCode()));
        //sort and update values here

        //insert the dictionaryCode of the given value into the encodedValues column
        encodedValues.push_back(dictionary.at(value));

        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::insert(const T& newValue) {

        T value = newValue;

        //insert the value along with its dictionaryCode into the dictionary map(if not already there)
        dictionary.insert(std::make_pair(value, getNewDictionaryCode()));
        //sort and update values here

        //insert the dictionaryCode of the given value into the encodedValues column
        encodedValues.push_back(dictionary.at(value));

        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool DictionaryEncoding<T>::insert(InputIterator first, InputIterator last) {
        /*
         * loop over all the values to be inserted
         * and insert them one by one
         */
        for (InputIterator it = first; it < last; it++) {
            if (!(this->insert(*it))) {
                return false;
            }
        }
        return true;
    }

    template<class T>
    const boost::any DictionaryEncoding<T>::get(TID tid) {
        //check for out of range tid
        if (tid < encodedValues.size()) {
            //loop over all the pairs in the dictionary to get the value that corresponds to the dictionaryCode at tid
            for (typename std::map < T, std::vector<bool> >::iterator it = dictionary.begin(); it != dictionary.end(); it++) {
                if (it->second == encodedValues.at(tid)) {
                    return it->first;
                }
            }
        } else {
            std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid << std::endl;
            return boost::any();
        }
    }

    template<class T>
    void DictionaryEncoding<T>::print() const throw () {
        std::cout << "| " << this->name_ << " |" << std::endl;
        std::cout << "________________________" << std::endl;

        for (uint64_t i = 0; i < encodedValues.size(); i++) {
            for (typename std::map < T, std::vector<bool> >::const_iterator it = dictionary.begin(); it != dictionary.end(); it++) {
                if (it->second == encodedValues.at(i)) {
                    std::cout << "| " << it->first << " |" << std::endl;
                }
            }
        }
    }

    template<class T >
    size_t DictionaryEncoding<T>::size() const throw () {
        return encodedValues.size();
    }

    template<class T>
    const ColumnPtr DictionaryEncoding<T>::copy() const {
        return ColumnPtr(new DictionaryEncoding<T>(*this));
    }

    template<class T>
    bool DictionaryEncoding<T>::update(TID tid, const boost::any & newValue) {
        //check for different type value or out of range tid
        if (typeid (T) != newValue.type() || tid >= encodedValues.size()) {
            return false;
        }

        T value = boost::any_cast<T>(newValue);

        //insert the newValue along with its dictionaryCode into the dictionary map(if not already there)
        dictionary.insert(std::make_pair(value, getNewDictionaryCode()));
        //update the tuple dictionaryCode with the dictionaryCode of the newValue
        encodedValues[tid] = dictionary.at(value);

        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::update(PositionListPtr tids, const boost::any& newValue) {
        //check for empty tid list, different type value or empty compressed column
        if (tids->empty() || typeid (T) != newValue.type() || encodedValues.empty()) {
            return false;
        }

        T value = boost::any_cast<T>(newValue);

        //insert the newValue along with its dictionaryCode into the dictionary map(if not already there)
        dictionary.insert(std::make_pair(value, getNewDictionaryCode()));

        //loop over the tids and update them one by one
        for (uint64_t i = 0; i < tids->size(); i++) {
            //update the tuple dictionaryCode with the dictionaryCode of the newValue
            encodedValues[tids->at(i)] = dictionary.at(value);
        }
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::remove(TID tid) {
        //check for out of range tid
        if (encodedValues.size() <= tid) {
            return false;
        }

        //delete the specified tuple(tid) from the encodedValues column
        encodedValues.erase(encodedValues.begin() + tid);
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::remove(PositionListPtr tids) {
        //check for empty tid list or empty compressed column
        if (tids->empty() || encodedValues.empty()) {
            return false;
        }

        //loop over the tids and remove them one by one
        for (uint64_t i = 0; i < tids->size(); i++) {
            if (!(this->remove(tids->at(i)))) {
                return false;
            }
        }
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::clearContent() {
        dictionary.clear();
        encodedValues.clear();
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::store(const std::string & path_) {
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile(path.c_str(), std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << dictionary;


        oa << encodedValues;

        outfile.flush();
        outfile.close();
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::load(const std::string & path_) {
        std::string path(path_);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile(path.c_str(), std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);
        ia >> dictionary;
        ia >> encodedValues;
        infile.close();


        return true;
    }

    template<class T >
    T & DictionaryEncoding<T>::operator[](const int tid) {
        static T t;
        //check for out of range tid
        if (tid < encodedValues.size()) {
            //loop over all the pairs in the dictionary to get the value that corresponds to the dictionaryCode at tid
            for (typename std::map < T, std::vector<bool> >::iterator it = dictionary.begin(); it != dictionary.end(); it++) {
                if (it->second == encodedValues.at(tid)) {
                    t = it->first;
                    return t;
                }
            }
        } else {
            std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid << std::endl;
            return t;
        }
    }

    template<class T>
    unsigned int DictionaryEncoding<T>::getSizeinBytes() const throw () {
        uint64_t size_in_bytes = sizeof (encodedValues);
        size_in_bytes += sizeof (dictionary);
        for (typename std::map < T, std::vector<bool> >::const_iterator it = dictionary.begin(); it != dictionary.end(); it++) {
            size_in_bytes += sizeof (it->first) + it->second.size() + sizeof (it->second);
        }

        for (uint64_t i = 0; i < encodedValues.size(); i++) {
            size_in_bytes += encodedValues[i].size() + sizeof (encodedValues[i]);
        }

        return size_in_bytes;
    }

    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB
