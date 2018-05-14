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

        /*! values*/
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

        if (newValue.empty()) return false;
        if (typeid (T) == newValue.type()) {
            T value = boost::any_cast<T>(newValue);
            if (!compressedValues.empty() && compressedValues.back().second == value) {
                compressedValues.back().first += 1;
            } else {
                compressedValues.push_back(std::make_pair(1, value));
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
        if (!compressedValues.empty() && compressedValues.back().second == newValue) {
            compressedValues.back().first += 1;
        } else {
            compressedValues.push_back(std::make_pair(1, newValue));
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
        if (compressedValues.empty()) return boost::any();

        int sum = 0;
        for (uint64_t i = 0; i <= compressedValues.size(); i++) {
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
        for (uint64_t i = 0; i <= compressedValues.size(); i++) {
            for (uint64_t j = 0; j < compressedValues.at(i).first; j++) {
                std::cout << "| " << compressedValues.at(i).second << " |" << std::endl;
            }
        }
    }

    template<class T>
    size_t RunLengthEncoding<T>::size() const throw () {

        return compressedValues.size();
    }

    template<class T>
    const ColumnPtr RunLengthEncoding<T>::copy() const {
        return ColumnPtr(new RunLengthEncoding<T>(*this));
    }

    template<class T>
    bool RunLengthEncoding<T>::update(TID tid, const boost::any& newValue) {
        if (compressedValues.empty()) return false;

        T value;
        if (typeid (T) == newValue.type()) {
            value = boost::any_cast<T>(newValue);
        } else {
            return false;
        }
        int sum = 0;
        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            sum += compressedValues.at(i).first;
            if (sum > tid) {
                if (compressedValues.at(i).first == 1) {
                    if (i - 1 >= 0 && compressedValues.at(i - 1).second == value) {
                        compressedValues.erase(compressedValues.begin() + i);
                        compressedValues.at(i - 1).first += 1;
                    } else if (i + 1 < compressedValues.size() && compressedValues.at(i + 1).second == value) {
                        compressedValues.erase(compressedValues.begin() + i);
                        compressedValues.at(i + 1).first += 1;
                    } else {
                        compressedValues.at(i).second = value;
                    }
                } else if (compressedValues.at(i).first > 1) {
                    if (i - 1 >= 0 && compressedValues.at(i - 1).first + 1 == tid) {
                        if (compressedValues.at(i - 1).second == value) {
                            compressedValues.at(i - 1).first += 1;
                            compressedValues.at(i).first -= 1;
                        } else {
                            compressedValues.insert(compressedValues.begin() + i, std::make_pair<uint64_t, T>(1, value));
                            compressedValues.at(i).first -= 1;
                        }
                    } else if (i + 1 < compressedValues.size() && compressedValues.at(i + 1).first - 1 == tid) {
                        if (compressedValues.at(i + 1).second == value) {
                            compressedValues.at(i + 1).first += 1;
                            compressedValues.at(i).first -= 1;
                        } else {
                            compressedValues.insert(compressedValues.begin() + i + 1, std::make_pair<uint64_t, T>(1, value));
                            compressedValues.at(i).first -= 1;
                        }
                    } else {
                        T oldValue = compressedValues.at(i).second;
                        compressedValues.insert(compressedValues.begin() + i, std::make_pair<uint64_t, T>(compressedValues.at(i).first - sum - tid - 1, oldValue));
                        compressedValues.at(i).first = 1;
                        compressedValues.at(i).second = value;
                        compressedValues.insert(compressedValues.begin() + i + 1, std::make_pair<uint64_t, T>(sum - tid, oldValue));
                    }
                }
                return true;
            }
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::update(PositionListPtr tids, const boost::any& newValue) {

        if (tids->empty() || compressedValues.empty() || newValue.empty()) {
            return false;
        }

        T value;
        if (typeid (T) == newValue.type()) {
            value = boost::any_cast<T>(newValue);
        } else {
            return false;
        }

        for (uint64_t i = 0; i < tids->size(); i++) {
            this->update(tids->at(i), newValue);
        }
        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(TID tid) {
        if (compressedValues.empty()) return false;

        int sum = 0;
        for (uint64_t i = 0; i < compressedValues.size(); i++) {
            sum += compressedValues.at(i).first;
            if (sum > tid) {
                if (compressedValues.at(i).first > 1) {
                    compressedValues.at(i).first -= 1;
                } else {
                    if (i - 1 >= 0 && i + 1 < compressedValues.size() &&
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

        if (tids->empty() || compressedValues.empty()) {
            return false;
        }

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
    bool RunLengthEncoding<T>::load(const std::string& path_) {
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
    T& RunLengthEncoding<T>::operator[](const int tid) {
        if (tid < compressedValues.size()) {
            int sum = 0;
            for (uint64_t i = 0; i <= compressedValues.size(); i++) {
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
        unsigned int size_in_bytes = 0;
        for (unsigned int i = 0; i < compressedValues.size(); ++i) {
            size_in_bytes += (compressedValues.at(i).second.size()) + sizeof (compressedValues.at(i).first);
        }
        return size_in_bytes;
    }

    /***************** End of Implementation Section ******************/



}; //end namespace CogaDB

