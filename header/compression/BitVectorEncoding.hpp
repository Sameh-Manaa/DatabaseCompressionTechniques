#pragma once

#include <core/compressed_column.hpp>
#include <utility>
#include <boost/serialization/utility.hpp>
#include <map>
#include <boost/serialization/map.hpp>

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

        /*!compressed values structure*/
        /*map<[VALUE],vector<RECORD_FLAG> >*/
        std::map<T, std::vector<bool> > valueBitVectorMap;

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
        //check for different type value
        if (typeid (T) != newValue.type()) {
            return false;
        }

        T value = boost::any_cast<T>(newValue);

        /*
         * loop over all the values and extend their bitvector with 1 value
         * (TRUE if value == newValue, FALSE otherwise)
         */
        bool valueFound = false;
        for (typename std::map < T, std::vector<bool> >::iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            if (it->first == value) {
                it->second.push_back(true);
                valueFound = true;
            } else {
                it->second.push_back(false);
            }
        }

        //if newValue not found then construct its bitvector and insert it into the valueBitVectorMap
        if (!valueFound) {
            uint64_t bitSetSize = 1;

            if (!valueBitVectorMap.empty()) {
                bitSetSize = valueBitVectorMap.begin()->second.size();
            }

            std::vector<bool> bitSet(bitSetSize, false);
            bitSet.at(bitSetSize - 1) = true;

            valueBitVectorMap[value] = bitSet;
        }
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::insert(const T& newValue) {

        T value = newValue;

        /*
         * loop over all the values and extend their bitvector with 1 value
         * (TRUE if value == newValue, FALSE otherwise)
         */
        bool valueFound = false;
        for (typename std::map < T, std::vector<bool> >::iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            if (it->first == value) {
                it->second.push_back(true);
                valueFound = true;
            } else {
                it->second.push_back(false);
            }
        }

        //if newValue not found then construct its bitvector and insert it into the valueBitVectorMap
        if (!valueFound) {
            uint64_t bitSetSize = 1;

            if (!valueBitVectorMap.empty()) {
                bitSetSize = valueBitVectorMap.begin()->second.size();
            }

            std::vector<bool> bitSet(bitSetSize, false);
            bitSet.at(bitSetSize - 1) = true;

            valueBitVectorMap[value] = bitSet;
        }
        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool BitVectorEncoding<T>::insert(InputIterator first, InputIterator last) {
        uint64_t sizeBeforeInsertion = valueBitVectorMap.empty() ? 0 : valueBitVectorMap.begin()->second.size();
        uint64_t sizeAfterInsertion = sizeBeforeInsertion + std::distance(first, last);

        //resize the bitvector for all the values to accommodate the new insertions
        for (typename std::map < T, std::vector<bool> >::iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            it->second.resize(sizeAfterInsertion, false);
        }

        /*
         * loop over all the values to be inserted, construct their bitvector 
         * and insert them into the valueBitVectorMap
         */
        uint64_t i = 0;
        for (InputIterator it = first; it < last; it++, i++) {

            std::vector<bool> bitSet(sizeAfterInsertion, false);

            valueBitVectorMap.insert(std::make_pair<T, std::vector<bool> >(*it, bitSet)).first->second.at(sizeBeforeInsertion + i) = true;
        }

        return true;
    }

    template<class T>
    const boost::any BitVectorEncoding<T>::get(TID tid) {
        //check for empty data or out of range tid
        if (valueBitVectorMap.empty() || valueBitVectorMap.begin()->second.size() <= tid) {
            return boost::any();
        }

        //loop over all the values check their bitvectors for the provided tid index set to TRUE
        for (typename std::map < T, std::vector<bool> >::const_iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            if (it->second.at(tid)) {
                return boost::any(it->first);
            }
        }

        return boost::any();
    }

    template<class T>
    void BitVectorEncoding<T>::print() const throw () {
        std::cout << "| " << this->name_ << " |" << std::endl;
        std::cout << "________________________" << std::endl;

        if (valueBitVectorMap.empty()) {
            return;
        }

        for (uint64_t i = 0; i < valueBitVectorMap.begin()->second.size(); i++) {
            for (typename std::map < T, std::vector<bool> >::const_iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
                if (it->second.at(i)) {
                    std::cout << "| " << it->first << " |" << std::endl;
                    break;
                }
            }
        }
    }

    template<class T>
    size_t BitVectorEncoding<T>::size() const throw () {
        if (valueBitVectorMap.empty()) {
            return 0;
        } else {
            return valueBitVectorMap.begin()->second.size();
        }
    }

    template<class T>
    const ColumnPtr BitVectorEncoding<T>::copy() const {
        return ColumnPtr(new BitVectorEncoding<T>(*this));
    }

    template<class T>
    bool BitVectorEncoding<T>::update(TID tid, const boost::any& newValue) {
        //check for empty compressed column, or different type value or out of range tid
        if (valueBitVectorMap.empty() || typeid (T) != newValue.type() || valueBitVectorMap.begin()->second.size() <= tid) {
            return false;
        }

        T value = boost::any_cast<T>(newValue);

        /*
         * loop over all the values and check their bitvectors for the provided tid index set to TRUE
         * flip it into FALSE
         * and set the bit at the provided ti index for the newValue to TRUE
         */
        bool valueUpdated = false;
        for (typename std::map < T, std::vector<bool> >::iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            if (it->second.at(tid)) {
                it->second.at(tid) = false;
                break;
            }

            if (it->first == value) {
                it->second.at(tid) = true;
                valueUpdated = true;
            }
        }

        /*
         * if the newValue not found then
         * construct its bitvector and insert it into the valueBitVectorMap
         */
        if (!valueUpdated) {
            uint64_t i = 0;

            std::vector<bool> bitSet(valueBitVectorMap.begin()->second.size(), false);

            valueBitVectorMap.insert(std::make_pair<T, std::vector<bool> >(value, bitSet)).first->second.at(tid) = true;
        }

        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::update(PositionListPtr tids, const boost::any& newValue) {
        //check for empty tid list, empty compressed column, or different type value
        if (tids->empty() || valueBitVectorMap.empty() || typeid (T) != newValue.type()) {
            return false;
        }

        T value = boost::any_cast<T>(newValue);

        //loop over the tids and update them one by one
        for (uint64_t i = 0; i < tids->size(); i++) {
            this->update(tids->at(i), newValue);
        }
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::remove(TID tid) {
        //check for empty compressed column, or out of range tid
        if (valueBitVectorMap.empty() || valueBitVectorMap.begin()->second.size() <= tid) {
            return false;
        }

        /*
         * loop over all the values
         * erase the bit corresponding to the provided tid index
         */
        for (typename std::map < T, std::vector<bool> >::iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            it->second.erase(it->second.begin() + tid);
        }

        //if all records have been deleted then clear the valueBitVectorMap
        if (valueBitVectorMap.begin()->second.size() == 0) {
            valueBitVectorMap.clear();
        }
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::remove(PositionListPtr tids) {
        //check for empty tid list or empty compressed column
        if (tids->empty() || valueBitVectorMap.empty()) {
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
    bool BitVectorEncoding<T>::clearContent() {
        valueBitVectorMap.clear();
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::store(const std::string& path_) {
        std::string path(path_);
        path += "/";
        path += this->name_;

        std::ofstream outfile(path.c_str(), std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << valueBitVectorMap;

        outfile.flush();
        outfile.close();
        return true;
    }

    template<class T>
    bool BitVectorEncoding<T>::load(const std::string& path_) {
        std::string path(path_);
        path += "/";
        path += this->name_;

        std::ifstream infile(path.c_str(), std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);

        ia >> valueBitVectorMap;

        infile.close();
        return true;
    }

    template<class T>
    T& BitVectorEncoding<T>::operator[](const int tid) {
        static T t;
        //check for empty data or out of range tid
        if (valueBitVectorMap.empty() || valueBitVectorMap.begin()->second.size() <= tid) {
            return t;
        }

        //loop over all the values check their bitvectors for the provided tid index set to TRUE
        for (typename std::map < T, std::vector<bool> >::const_iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            if (it->second.at(tid)) {

                return t = it->first;
            }
        }

        return t;
    }

    template<class T>
    unsigned int BitVectorEncoding<T>::getSizeinBytes() const throw () {
        uint64_t size_in_bytes = sizeof (valueBitVectorMap);
        for (typename std::map < T, std::vector<bool> >::const_iterator it = valueBitVectorMap.begin(); it != valueBitVectorMap.end(); it++) {
            size_in_bytes += sizeof (it->first) + it->second.size() + sizeof (it->second);
        }
        return size_in_bytes;
    }

    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB

