#pragma once

#include <core/compressed_column.hpp>
#include <utility>
#include <boost/serialization/utility.hpp>
#include <map>
#include <boost/serialization/map.hpp>

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



        virtual T& operator[](const int index);

    private:

        /*compressed values structure*/
        /*map<[VALUE],[DICTIONARY_CODE]>*/
        std::map<T, std::string > dictionary;
        /*vector<[DICTIONARY_CODE]>*/
        std::vector<std::string > values;
    };

    /***************** Start of Implementation Section ******************/


    template<class T>
    DictionaryEncoding<T>::DictionaryEncoding(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type) {

    }

    template<class T>
    DictionaryEncoding<T>::~DictionaryEncoding() {

    }

    template<class T>
    bool DictionaryEncoding<T>::insert(const boost::any& newValue) {

        if (typeid (T) == newValue.type()) {
            T value = boost::any_cast<T>(newValue);

            bool valueFound = false;

            if (dictionary.find(value) != dictionary.end()) {
                valueFound = true;
            }

            if (!valueFound) {
                std::string code = std::bitset<8>(dictionary.size()).to_string(); //to binary
                dictionary[value] = code;
                //sort here
                //update values
            }

            values.push_back(dictionary.at(value));

            return true;
        } else {
            return false;
        }
    }

    template<class T>
    bool DictionaryEncoding<T>::insert(const T& newValue) {

        if (newValue.empty()) return false;

        T value = newValue;

        bool valueFound = false;

        if (dictionary.find(value) != dictionary.end()) {
            valueFound = true;
        }

        if (!valueFound) {
            std::string code = std::bitset<8>(dictionary.size()).to_string(); //to binary
            dictionary[value] = code;
            //sort here
            //update values
        }

        values.push_back(dictionary.at(value));

        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool DictionaryEncoding<T>::insert(InputIterator, InputIterator) {

        return true;
    }

    template<class T>
    const boost::any DictionaryEncoding<T>::get(TID tid) {
        return boost::any();
    }

    template<class T>
    void DictionaryEncoding<T>::print() const throw () {

    }

    template<class T>
    size_t DictionaryEncoding<T>::size() const throw () {

        return dictionary.size();
    }

    template<class T>
    const ColumnPtr DictionaryEncoding<T>::copy() const {
        return ColumnPtr(new DictionaryEncoding<T>(*this));
    }

    template<class T>
    bool DictionaryEncoding<T>::update(TID tid, const boost::any& newValue) {

        if (dictionary.empty()) return false;

        T value;
        if (typeid (T) == newValue.type()) {
            value = boost::any_cast<T>(newValue);
        } else {
            return false;
        }

        static T t;
        for (typename std::map<T, std::string>::iterator it = dictionary.begin(); it != dictionary.end(); it++) {
            if (it->second == values.at(tid)) {
                t = it->first;
                dictionary[value] = dictionary.at(t);
                dictionary.erase(t);
                return true;
            }
        }
        return false;
    }

    template<class T>
    bool DictionaryEncoding<T>::update(PositionListPtr, const boost::any&) {
        return false;
    }

    template<class T>
    bool DictionaryEncoding<T>::remove(TID tid) {
        values.erase(values.begin() + tid);
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::remove(PositionListPtr) {
        return false;
    }

    template<class T>
    bool DictionaryEncoding<T>::clearContent() {
        dictionary.clear();
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::store(const std::string& path_) {
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile(path.c_str(), std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << dictionary;


        oa << values;

        outfile.flush();
        outfile.close();
        return true;
    }

    template<class T>
    bool DictionaryEncoding<T>::load(const std::string& path_) {
        std::string path(path_);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile(path.c_str(), std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);
        ia >> dictionary;
        ia >> values;
        infile.close();


        return true;
    }

    template<class T>
    T& DictionaryEncoding<T>::operator[](const int tid) {
        static T t;
        if (dictionary.empty()) return t;

        if (tid < values.size()) {

            for (typename std::map<T, std::string>::iterator it = dictionary.begin(); it != dictionary.end(); it++) {
                if (it->second == values.at(tid)) {
                    t = it->first;
                    return t;
                }
            }
        } else {
            std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid << std::endl;
        }
        return t;
    }

    template<class T>
    unsigned int DictionaryEncoding<T>::getSizeinBytes() const throw () {
        return 0; //return values_.capacity()*sizeof(T);
    }

    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB
