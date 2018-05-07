
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>
#include <utility>

namespace CoGaDB {

    /*!
     *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
     */
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

        /*! values*/
        //std::vector<T> compressedValues_;
        std::vector<std::pair<uint64_t, T> > compressedValues_;
        //std::vector<int> countValues_;
        //int totalCount = 0;

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

        if (newValue.empty()) return false;
        if (typeid (T) == newValue.type()) {
            T value = boost::any_cast<T>(newValue);
            if (!compressedValues_.empty() && compressedValues_.back().second == value) {
                compressedValues_.back().first += 1;
            } else {
                compressedValues_.push_back(std::make_pair(1, value));
                //countValues_.push_back(1);
            }
            //totalCount++;
            return true;
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::insert(const T& newValue) {
        if (newValue.empty()) return false;
        if (!compressedValues_.empty() && compressedValues_.back().second == newValue) {
            compressedValues_.back().first += 1;
        } else {
            compressedValues_.push_back(std::make_pair(1, newValue));
            //countValues_.push_back(1);
        }
        //totalCount++;
        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool RunLengthEncoding<T>::insert(InputIterator first, InputIterator last) {
        for (InputIterator it = first; it < last; it++) {
            this->insert(*it);
        }
        return true;
    }

    template<class T>
    const boost::any RunLengthEncoding<T>::get(TID tid) {
        if (compressedValues_.empty()) return boost::any();
        //if (tid < compressedValues_.size()) {
        int sum = 0;
        for (uint64_t i = 0; i <= compressedValues_.size(); i++) {
            sum += compressedValues_.at(i).first;
            if (sum > tid) {
                return boost::any(compressedValues_.at(i).second);
            }
        }
        //} else {
        //    std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid << std::endl;
        //}
        return boost::any();
    }

    template<class T>
    void RunLengthEncoding<T>::print() const throw () {
        std::cout << "| " << this->name_ << " |" << std::endl;
        std::cout << "________________________" << std::endl;
        for (uint64_t i = 0; i <= compressedValues_.size(); i++) {
            for (uint64_t j = 0; j < compressedValues_.at(i).first; j++) {
                std::cout << "| " << compressedValues_.at(i).second << " |" << std::endl;
            }
        }
    }

    template<class T>
    size_t RunLengthEncoding<T>::size() const throw () {

        return compressedValues_.size();
    }

    template<class T>
    const ColumnPtr RunLengthEncoding<T>::copy() const {
        return ColumnPtr(new RunLengthEncoding<T>(*this));
    }

    template<class T>
    bool RunLengthEncoding<T>::update(TID tid, const boost::any& newValue) {
        if (compressedValues_.empty()) return false;

        T value;
        if (typeid (T) == newValue.type()) {
            value = boost::any_cast<T>(newValue);
        } else {
            return false;
        }
        int sum = 0;
        for (uint64_t i = 0; i < compressedValues_.size(); i++) {
            sum += compressedValues_.at(i).first;
            if (sum > tid) {
                if (compressedValues_.at(i).first == 1) {
                    if (i - 1 >= 0 && compressedValues_.at(i - 1).second == value) {
                        compressedValues_.erase(compressedValues_.begin() + i);
                        compressedValues_.at(i - 1).first += 1;
                    } else if (i + 1 < compressedValues_.size() && compressedValues_.at(i + 1).second == value) {
                        compressedValues_.erase(compressedValues_.begin() + i);
                        compressedValues_.at(i + 1).first += 1;
                    } else {
                        compressedValues_.at(i).second = value;
                    }
                } else if (compressedValues_.at(i).first > 1) {
                    if (i - 1 >= 0 && compressedValues_.at(i - 1).first + 1 == tid) {
                        if (compressedValues_.at(i - 1).second == value) {
                            compressedValues_.at(i - 1).first += 1;
                            compressedValues_.at(i).first -= 1;
                        } else {
                            compressedValues_.insert(compressedValues_.begin() + i, std::make_pair<uint64_t, T>(1, value));
                            compressedValues_.at(i).first -= 1;
                        }
                    } else if (i + 1 < compressedValues_.size() && compressedValues_.at(i + 1).first - 1 == tid) {
                        if (compressedValues_.at(i + 1).second == value) {
                            compressedValues_.at(i + 1).first += 1;
                            compressedValues_.at(i).first -= 1;
                        } else {
                            compressedValues_.insert(compressedValues_.begin() + i + 1, std::make_pair<uint64_t, T>(1, value));
                            compressedValues_.at(i).first -= 1;
                        }
                    } else {
                        T oldValue = compressedValues_.at(i).second;
                        compressedValues_.insert(compressedValues_.begin() + i, std::make_pair<uint64_t, T>(compressedValues_.at(i).first - sum - tid - 1, oldValue));
                        compressedValues_.at(i).first = 1;
                        compressedValues_.at(i).second = value;
                        compressedValues_.insert(compressedValues_.begin() + i + 1, std::make_pair<uint64_t, T>(sum - tid, oldValue));
                    }
                }
                return true;
            }
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::update(PositionListPtr, const boost::any&) {
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(TID tid) {
        if (compressedValues_.empty()) return false;

        int sum = 0;
        for (uint64_t i = 0; i < compressedValues_.size(); i++) {
            sum += compressedValues_.at(i).first;
            if (sum > tid) {
                if (compressedValues_.at(i).first > 1) {
                    compressedValues_.at(i).first-=1;
                }else{
                    if(i-1 >= 0 && i+1 < compressedValues_.size() && 
                            compressedValues_.at(i-1).second == compressedValues_.at(i+1).second ){
                        compressedValues_.at(i-1).first+=compressedValues_.at(i+1).first;
                        compressedValues_.erase(compressedValues_.begin()+i+1);
                        compressedValues_.erase(compressedValues_.begin()+i);
                    }else{
                        compressedValues_.erase(compressedValues_.begin()+i);
                    }
                }
                return true;
            }
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(PositionListPtr) {
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::clearContent() {
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::store(const std::string&) {
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::load(const std::string&) {
        return false;
    }

    template<class T>
    T& RunLengthEncoding<T>::operator[](const int tid) {
        if (tid < compressedValues_.size()) {
            int sum = 0;
            for (uint64_t i = 0; i <= compressedValues_.size(); i++) {
                sum += compressedValues_.at(i).first;
                if (sum > tid) {
                    return compressedValues_.at(i).second;
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
        unsigned int size_in_bytes = 0;
        for (unsigned int i = 0; i < compressedValues_.size(); ++i) {
            size_in_bytes += compressedValues_.at(i).second.capacity() + sizeof (compressedValues_.at(i).first);
        }
        //return values_.size()*sizeof(T);
        return size_in_bytes;
    }

    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB

