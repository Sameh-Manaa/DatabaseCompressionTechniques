#pragma once

#include <core/compressed_column.hpp>
#include <utility>
#include <boost/serialization/utility.hpp>

namespace CoGaDB {

    template<class T>
    class BitVectorEncoding : public CompressedColumn<T> {
    public:
        /***************** constructors and destructor *****************/
        BitVectorEncoding(const std::string& name, AttributeType db_type);
        virtual ~BitVectorEncoding();

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

        /*! values*/
        std::vector<std::pair<T, std::vector<bool> > > compressedValues;

    };

    /***************** Start of Implementation Section ******************/


    template<class T>
    BitVectorEncoding<T>::BitVectorEncoding(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type) {

    }

    template<class T>
    BitVectorEncoding<T>::~BitVectorEncoding() {

    }

    template<class T>
    bool BitVectorEncoding<T>::insert(const boost::any& newValue) {

        if (newValue.empty()) return false;

        if (typeid (T) == newValue.type()) {
            T value = boost::any_cast<T>(newValue);

            bool valueFound = false;
            for (uint64_t i = 0; i < compressedValues.size(); i++) {
                if (compressedValues.at(i).first == value) {
                    compressedValues.at(i).second.push_back(true);
                    valueFound = true;
                } else {
                    compressedValues.at(i).second.push_back(false);
                }
            }
            if (!valueFound) {
                std::vector<bool> bitSet;
                uint64_t bitSetSize = 1;

                if (!compressedValues.empty()) {
                    bitSetSize = compressedValues.at(0).second.size();
                }

                for (uint64_t i = 0; i < bitSetSize; i++) {
                    if (i == bitSetSize - 1) {
                        bitSet.push_back(true);
                    } else {
                        bitSet.push_back(false);
                    }
                }

                compressedValues.push_back(std::make_pair(value, bitSet));
            }
            return true;
        }
        return false;
    }

    template<class T>
    bool BitVectorEncoding<T>::insert(const T& newValue) {

        if (newValue.empty()) return false;

        T value = newValue;

        bool valueFound = false;
        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            if (compressedValues.at(i).first == value) {
                compressedValues.at(i).second.push_back(true);
                valueFound = true;
            } else {
                compressedValues.at(i).second.push_back(false);
            }
        }
        if (!valueFound) {
            std::vector<bool> bitSet;
            uint64_t bitSetSize = 1;

            if (!compressedValues.empty()) {
                bitSetSize = compressedValues.at(0).second.size();
            }

            for (uint64_t i = 0; i < bitSetSize; i++) {
                if (i == bitSetSize - 1) {
                    bitSet.push_back(true);
                } else {
                    bitSet.push_back(false);
                }
            }

            compressedValues.push_back(std::make_pair(value, bitSet));
        }
        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool BitVectorEncoding<T>::insert(InputIterator, InputIterator) {

        return true;
    }

    template<class T>
    const boost::any BitVectorEncoding<T>::get(TID tid) {
        if (compressedValues.empty()) return boost::any();

        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            if (compressedValues.at(i).second.at(tid)) {
                return boost::any(compressedValues.at(i).first);
            }
        }
        return boost::any();
    }

    template<class T>
    void BitVectorEncoding<T>::print() const throw () {

    }

    template<class T>
    size_t BitVectorEncoding<T>::size() const throw () {

        return compressedValues.size();
    }

    template<class T>
    const ColumnPtr BitVectorEncoding<T>::copy() const {
        return ColumnPtr(new BitVectorEncoding<T>(*this));
    }

    template<class T>
    bool BitVectorEncoding<T>::update(TID tid, const boost::any& newValue) {
        if (compressedValues.empty()) return false;

        T value;
        if (typeid (T) == newValue.type()) {
            value = boost::any_cast<T>(newValue);
        } else {
            return false;
        }

        bool valueUpdated = false;
        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            if (compressedValues.at(i).second.at(tid)) {
                compressedValues.at(i).second.at(tid) = false;
            }

            if (compressedValues.at(i).first == value) {
                compressedValues.at(i).second.at(tid) = true;
                valueUpdated = true;
            }
        }

        if (!valueUpdated) {
            std::vector<bool> bitSet;
            uint64_t bitSetSize = 1;

            if (!compressedValues.empty()) {
                bitSetSize = compressedValues.at(0).second.size();
            }

            for (uint64_t i = 0; i < bitSetSize; i++) {
                if (i == tid) {
                    bitSet.push_back(true);
                } else {
                    bitSet.push_back(false);
                }
            }

            compressedValues.push_back(std::make_pair(value, bitSet));
        }

        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::update(PositionListPtr, const boost::any&) {
        return false;
    }

    template<class T>
    bool BitVectorEncoding<T>::remove(TID tid) {
        if (compressedValues.empty() || compressedValues.at(0).second.size() <= tid) return false;

        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            compressedValues.at(i).second.erase(compressedValues.at(i).second.begin() + tid);
        }
        
        if(compressedValues.at(0).second.size() == 0){
            compressedValues.clear();
        }
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::remove(PositionListPtr) {
        return false;
    }

    template<class T>
    bool BitVectorEncoding<T>::clearContent() {
        compressedValues.clear();
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::store(const std::string& path_) {
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile(path.c_str(), std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << compressedValues;

        outfile.flush();
        outfile.close();
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::load(const std::string& path_) {
        std::string path(path_);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile(path.c_str(), std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);
        ia >> compressedValues;
        infile.close();


        return true;
    }

    template<class T>
    T& BitVectorEncoding<T>::operator[](const int tid) {
        static T t;
        if (compressedValues.empty()) return t;

        if (tid < compressedValues.size()) {
            for (uint64_t i = 0; i < compressedValues.size(); i++) {
                if (compressedValues.at(i).second.at(tid)) {
                    return compressedValues.at(i).first;
                }
            }
        } else {
            std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid << std::endl;
        }
        return t;
    }

    template<class T>
    unsigned int BitVectorEncoding<T>::getSizeinBytes() const throw () {
        return 0; //return values_.capacity()*sizeof(T);
    }

    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB

