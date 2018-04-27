
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>

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
        std::vector<T> compressedValues_;
        std::vector<int> countValues_;
        int totalCount = 0;

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
            if (compressedValues_[compressedValues_.capacity()] == value) {
                countValues_[countValues_.capacity()] += 1;
            } else {
                compressedValues_.push_back(value);
                countValues_.push_back(1);
            }
            totalCount++;
            return true;
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::insert(const T& newValue) {
        if (!compressedValues_.empty() && compressedValues_[compressedValues_.capacity()] == newValue) {
            countValues_[countValues_.capacity()] += 1;
        } else {
            compressedValues_.push_back(newValue);
            countValues_.push_back(1);
        }
        totalCount++;
        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool RunLengthEncoding<T>::insert(InputIterator, InputIterator) {

        return true;
    }

    template<class T>
    const boost::any RunLengthEncoding<T>::get(TID tid) {

        if (tid < totalCount) {
            int sum = 0;
            for (std::size_t i = 0; i != (sizeof countValues_); i++) {
                sum += countValues_[i];
                if (sum >= tid) {
                    return boost::any(compressedValues_[tid]);
                }
            }
        } else {
            std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid << std::endl;
        }
        return boost::any();
    }

    template<class T>
    void RunLengthEncoding<T>::print() const throw () {
        std::cout << "| " << this->name_ << " |" << std::endl;
        std::cout << "________________________" << std::endl;
        for (std::size_t i = 0; i != (sizeof countValues_ ); i++) {
            for (int j = 0; j < countValues_[i]; j++) {
                std::cout << "| " << compressedValues_[i] << " |" << std::endl;
            }
        }
    }

    template<class T>
    size_t RunLengthEncoding<T>::size() const throw () {

        return totalCount;
    }

    template<class T>
    const ColumnPtr RunLengthEncoding<T>::copy() const {

        return ColumnPtr();
    }

    template<class T>
    bool RunLengthEncoding<T>::update(TID, const boost::any&) {
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::update(PositionListPtr, const boost::any&) {
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(TID) {
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
    T& RunLengthEncoding<T>::operator[](const int) {
        static T t;
        return t;
    }

    template<class T>
    unsigned int RunLengthEncoding<T>::getSizeinBytes() const throw () {
        return sizeof(T) * totalCount;
    }

    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB

