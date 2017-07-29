#include "Timer.hpp"


/* Class Constructors & Destructor */
Timer::Timer() 
    : startClock{std::clock()}, startTime{boost::posix_time::second_clock::local_time()}, title("Untitle"), wall_ms_string("0"), cpu_ms_string("0") {

}


Timer::Timer(const std::string& title) 
    : startClock{std::clock()}, startTime{boost::posix_time::second_clock::local_time()}, title(title), wall_ms_string("0"), cpu_ms_string("0") {

}


Timer::~Timer() {

}


/* Public Methods */
std::string Timer::result() {
    auto clocks = std::clock() - startClock;
    double millisec_clock = clocks / (CLOCKS_PER_SEC / 1000);
    double sec_clock = clocks / CLOCKS_PER_SEC;
    double minute_clock = sec_clock / 60.0;
    double hour_clock = minute_clock / 60.0;

    boost::posix_time::time_duration diff = boost::posix_time::second_clock::local_time() - startTime;
    auto millisecs = diff.total_milliseconds();
    auto secs = millisecs / 1000.0;
    auto mins = secs / 60.0;
    auto hours = mins / 60.0;

    this->wall_ms_string = to_string_with_precision(millisecs, 3);
    this->cpu_ms_string = to_string_with_precision(millisec_clock, 3);

    return
    	"Wall time for \"" + title + "\": " +
		to_string_with_precision(millisecs, 3) + " ms; " +
		to_string_with_precision(secs, 3) + " s; " +
    	to_string_with_precision(mins, 3) + " m; " +
    	to_string_with_precision(hours, 3) + " h\n" +
    	"CPU time for \"" + title + "\": " +
        to_string_with_precision(millisec_clock, 3) + " ms; " +
        to_string_with_precision(sec_clock, 3) + " s; " +
        to_string_with_precision(minute_clock, 3) + " m; " +
        to_string_with_precision(hour_clock, 3) + " h";
}


/* Private Methods */
template <typename T>
std::string Timer::to_string_with_precision(const T a_value, const int& n) {
    std::ostringstream out;
    out << std::fixed;
    out << std::setprecision(n) << a_value;
    return out.str();
}
