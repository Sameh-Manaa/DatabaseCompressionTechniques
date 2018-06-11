#pragma once

#include <core/compressed_column.hpp>
#include <utility>
#include <boost/serialization/utility.hpp>

namespace CoGaDB {

    template<class T>
    class RunLengthEncoding : public CompressedColumn<T> {
    public:
        /***************** constructors and destructor *****************/
        RunLengthEncoding(const std::string& name, AttributeType db_type);
        virtual ~RunLengthEncoding();

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
        /*vector<pair<[RUN_LENGTH],[VALUE]> >*/
        std::vector<std::pair<uint64_t, T> > compressedValues;

    };

    /***************** Start of Implementation Section ******************/


    template<class T>
    RunLengthEncoding<T>::RunLengthEncoding(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type) {

    }

    template<class T>
    RunLengthEncoding<T>::~RunLengthEncoding() {

    }

    template<class T>
    bool RunLengthEncoding<T>::insert(const boost::any& newValue) {
        //check for different type value
        if (typeid (T) != newValue.type()) {
            return false;
        }
        T value = boost::any_cast<T>(newValue);
        /*
         * if the newValue is same as last inserted value then increase runLength by 1
         * else insert a new record with runLength = 1 associated with the newValue
         */
        if (!compressedValues.empty() && compressedValues.back().second == value) {
            compressedValues.back().first += 1;
        } else {
            compressedValues.push_back(std::make_pair(1, value));
        }
        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::insert(const T& newValue) {
        /*
         * if the newValue is same as last inserted value then increase runLength by 1
         * else insert a new record with runLength = 1 associated with the newValue
         */
        if (!compressedValues.empty() && compressedValues.back().second == newValue) {
            compressedValues.back().first += 1;
        } else {
            compressedValues.push_back(std::make_pair(1, newValue));
        }
        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool RunLengthEncoding<T>::insert(InputIterator first, InputIterator last) {
        const T& iteratorValue = *first;
        uint64_t runLength = 0;
        /*
         * readjust runLength if iteratorValue is same as last inserted value
         * set runLength = runLength of last inserted value
         */
        if (compressedValues.back().second == iteratorValue) {
            runLength = compressedValues.back().first;
            compressedValues.erase(compressedValues.back());
        }
        /*
         * if currentIteratorValue[*it] is same as lastIteratorValue[iteratorValue] then increment runLength by 1
         * else insert a new record with runLength associated with the lastIteratorValue 
         *      and set iteratorValue to the currentIteratorValue and runLength to 1
         */
        for (InputIterator it = first; it < last; it++) {
            if (*it == iteratorValue) {
                runLength++;
            } else {
                compressedValues.push_back(std::make_pair(runLength, iteratorValue));
                iteratorValue = *it;
                runLength = 1;
            }
        }
        compressedValues.push_back(std::make_pair(runLength, iteratorValue));
        return true;
    }

    template<class T>
    const boost::any RunLengthEncoding<T>::get(TID tid) {
        //check for empty data
        if (compressedValues.empty()) {
            return boost::any();
        }

        //loop and accumulate sum over the runLength value till the sum exceeds the tuple id
        int sum = 0;
        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            sum += compressedValues.at(i).first;
            if (sum > tid) {
                return boost::any(compressedValues.at(i).second);
            }
        }
        return boost::any();
    }

    template<class T>
    void RunLengthEncoding<T>::print() const throw () {
        std::cout << "| " << this->name_ << " |" << std::endl;
        std::cout << "________________________" << std::endl;
        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            for (uint64_t j = 0; j < compressedValues.at(i).first; j++) {
                std::cout << "| " << compressedValues.at(i).second << " |" << std::endl;
            }
        }
    }

    template<class T>
    size_t RunLengthEncoding<T>::size() const throw () {
        int sum = 0;
        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            sum += compressedValues.at(i).first;
        }
        return sum;
    }

    template<class T>
    const ColumnPtr RunLengthEncoding<T>::copy() const {
        return ColumnPtr(new RunLengthEncoding<T>(*this));
    }

    template<class T>
    bool RunLengthEncoding<T>::update(TID tid, const boost::any& newValue) {
        //check for empty compressed column, or different type value
        if (compressedValues.empty() || typeid (T) != newValue.type()) {
            return false;
        }
        T value = boost::any_cast<T>(newValue);

        int sum = 0;
        for (int64_t i = 0; i < compressedValues.size(); i++) {
            sum += compressedValues.at(i).first;
            if (sum > tid) {
                //if newValue equal to the current value then do nothing
                if (compressedValues.at(i).second == value) {
                    return true;
                }
                //if the tuple has a runLength of 1
                if (compressedValues.at(i).first == 1) {
                    //if newValue is same as previous tuple and next tuple value then combine the three tuples
                    if (i >= 1 && compressedValues.at(i - 1).second == value &&
                            i + 1 < compressedValues.size() && compressedValues.at(i + 1).second == value) {
                        compressedValues.at(i - 1).first += 1 + compressedValues.at(i + 1).first;
                        compressedValues.erase(compressedValues.begin() + i);
                        compressedValues.erase(compressedValues.begin() + i + 1);
                    }//if newValue is same as previous tuple only then combine the two tuples
                    else if (i >= 1 && compressedValues.at(i - 1).second == value) {
                        compressedValues.erase(compressedValues.begin() + i);
                        compressedValues.at(i - 1).first += 1;
                    }//if newValue is same as next tuple only then combine the two tuples
                    else if (i + 1 < compressedValues.size() && compressedValues.at(i + 1).second == value) {
                        compressedValues.erase(compressedValues.begin() + i);
                        compressedValues.at(i + 1).first += 1;
                    }//else update the tuple with the new value
                    else {
                        compressedValues.at(i).second = value;
                    }
                }//if the tuple has a runLength greater than 1 
                else if (compressedValues.at(i).first > 1) {
                    /*
                     * if updated tuple is located first in its compressed set
                     */
                    if (sum - compressedValues.at(i).first == tid) {
                        /* 
                         * if the tuple is not in the first compressed set 
                         * and it has the same value as preceding compressed set of tuples 
                         * then decrement the runLength of its compressed set by 1
                         * and insert it into the preceding set
                         */
                        if (i > 0 && compressedValues.at(i - 1).second == value) {
                            compressedValues.at(i - 1).first += 1;
                            compressedValues.at(i).first -= 1;
                            if (compressedValues.at(i).first == 0) {
                                compressedValues.erase(compressedValues.begin() + i);
                            }
                        }/*
                          * else insert a new compressed set with runLength 1 with the new value 
                          * and decrement the past set runLength by 1 
                          */
                        else {
                            compressedValues.insert(compressedValues.begin() + i, std::make_pair<uint64_t, T>(1, value));
                            compressedValues.at(i + 1).first -= 1;
                            if (compressedValues.at(i + 1).first == 0) {
                                compressedValues.erase(compressedValues.begin() + i + 1);
                            }
                        }
                    }/*
                      * if updated tuple is located last in its compressed set
                      */
                    else if (sum - 1 == tid) {
                        /* 
                         * if the tuple is not in the last compressed set 
                         * and it has the same value as following compressed set of tuples 
                         * then decrement the runLength of its compressed set by 1
                         * and insert it into the following set
                         */
                        if (i < (compressedValues.size() - 1) && compressedValues.at(i + 1).second == value) {
                            compressedValues.at(i + 1).first += 1;
                            compressedValues.at(i).first -= 1;
                            if (compressedValues.at(i).first == 0) {
                                compressedValues.erase(compressedValues.begin() + i);
                            }
                        }/*
                          * else insert a new compressed set with runLength 1 with the new value 
                          * and decrement the past set runLength by 1 
                          */
                        else {
                            compressedValues.insert(compressedValues.begin() + i + 1, std::make_pair<uint64_t, T>(1, value));
                            compressedValues.at(i).first -= 1;
                            if (compressedValues.at(i).first == 0) {
                                compressedValues.erase(compressedValues.begin() + i);
                            }
                        }
                    }/*
                      * else (when tuple is in the middle of a set with runLength greater than 1)
                      * then split the set into 3 sets, the middle one of them is the set with the updated value
                      */
                    else {
                        T oldValue = compressedValues.at(i).second;
                        compressedValues.insert(compressedValues.begin() + i, std::make_pair<uint64_t, T>(compressedValues.at(i).first - (sum - tid), oldValue));
                        compressedValues.at(i + 1).first = 1;
                        compressedValues.at(i + 1).second = value;
                        compressedValues.insert(compressedValues.begin() + i + 2, std::make_pair<uint64_t, T>(sum - tid - 1, oldValue));
                    }
                }
                return true;
            }
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::update(PositionListPtr tids, const boost::any& newValue) {
        //check for empty tid list, empty compressed column, or different type value
        if (tids->empty() || compressedValues.empty() || typeid (T) != newValue.type()) {
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
    bool RunLengthEncoding<T>::remove(TID tid) {
        if (compressedValues.empty()) {
            return false;
        }

        int sum = 0;
        for (int64_t i = 0; i < compressedValues.size(); i++) {
            sum += compressedValues.at(i).first;
            if (sum > tid) {
                //if runLength > 1 then decrement by 1
                if (compressedValues.at(i).first > 1) {
                    compressedValues.at(i).first -= 1;
                }//else(when runLength == 1) then delete the tuple and check for possible merge
                else {
                    //if merge is possible then merge
                    if (i >= 1 && i < compressedValues.size() - 1 &&
                            compressedValues.at(i - 1).second == compressedValues.at(i + 1).second) {
                        compressedValues.at(i - 1).first += compressedValues.at(i + 1).first;
                        compressedValues.erase(compressedValues.begin() + i + 1);
                        compressedValues.erase(compressedValues.begin() + i);
                    } else {
                        compressedValues.erase(compressedValues.begin() + i);
                    }
                }
                return true;
            }
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(PositionListPtr tids) {
        //check for empty tid list or empty compressed column
        if (tids->empty() || compressedValues.empty()) {
            return false;
        }

        //loop over the tids and remove them one by one
        for (uint64_t i = 0; i < tids->size(); i++) {
            this->remove(tids->at(i));
        }
        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::clearContent() {
        compressedValues.clear();
        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::store(const std::string& path_) {
        std::string path(path_);
        path += "/";
        path += this->name_;

        std::ofstream outfile(path.c_str(), std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << compressedValues;

        outfile.flush();
        outfile.close();
        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::load(const std::string& path_) {
        std::string path(path_);
        path += "/";
        path += this->name_;

        std::ifstream infile(path.c_str(), std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);

        ia >> compressedValues;

        infile.close();
        return true;
    }

    template<class T>
    T& RunLengthEncoding<T>::operator[](const int tid) {
        if (tid < compressedValues.size()) {
            int sum = 0;
            for (uint64_t i = 0; i < compressedValues.size(); i++) {
                sum += compressedValues.at(i).first;
                if (sum > tid) {
                    return compressedValues.at(i).second;
                }
            }
        } else {
            std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid << std::endl;
        }
        static T t;
        return t;
    }

    template<class T>
    unsigned int RunLengthEncoding<T>::getSizeinBytes() const throw () {
        uint64_t size_in_bytes = 0;
        for (uint64_t i = 0; i < compressedValues.size(); ++i) {
            size_in_bytes += sizeof (compressedValues.at(i).second) + sizeof (compressedValues.at(i).first);
        }
        return size_in_bytes;
    }
    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB

