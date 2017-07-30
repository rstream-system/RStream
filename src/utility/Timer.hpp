#ifndef TIMER_HPP
#define TIMER_HPP


#include "boost/date_time/posix_time/posix_time.hpp"
#include "../common/RStreamCommon.hpp"


// This class represents a timer to calculate the time elapsed
class Timer {
    
    public:
        /* Class Constructors & Destructor */
        Timer();
        Timer(const std::string& title);
        virtual ~Timer();


        /* Public Methods */
        std::string result();

        template <typename T>
        static std::string to_string_with_precision(const T a_value, const int& n);


        inline std::string getWallTimeString(){
        	return this->wall_ms_string;
        }

        inline std::string getCPUTimeString(){
        	return this->cpu_ms_string;
        }

    private:
        /* Declaring Variables */
        std::clock_t startClock;
        boost::posix_time::ptime startTime;
        std::string title;

        std::string wall_ms_string;
        std::string cpu_ms_string;

};

#endif // TIMER_HPP
