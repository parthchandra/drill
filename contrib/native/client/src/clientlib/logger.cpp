/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "boost/date_time/posix_time/posix_time.hpp"
#include "boost/thread.hpp"
#include "utils.hpp"
#include "logger.hpp"

namespace Drill{

/* 
 * Creates a single instance of the logger the first time this is called 
 */
/*  static */ boost::mutex g_logMutex;
Logger& getLogger() {
    boost::lock_guard<boost::mutex> logLock(g_logMutex); 
    static Logger* logger = new Logger();
    return *logger;
}

std::string getTime(){
    return to_simple_string(boost::posix_time::second_clock::local_time());
}

std::string getTid(){
    return boost::lexical_cast<std::string>(boost::this_thread::get_id());
}

//logLevel_t Logger::s_level=LOG_ERROR;
//std::ostream* Logger::s_pOutStream=NULL;
//std::ofstream* Logger::s_pOutFileStream=NULL;
//std::string Logger::s_filepath;
//boost::mutex Logger::s_logMutex;

void Logger::init(const char* path){

    static bool initialized=false;
    boost::lock_guard<boost::mutex> logLock(s_logMutex); 
    if(!initialized && path!=NULL && s_filepath.empty()) {
        std::string fullname = path;
        size_t lastindex = fullname.find_last_of(".");
        std::string filename;
        if(lastindex != std::string::npos){
        filename = fullname.substr(0, lastindex)
            +std::to_string(Utils::s_randomNumber())
            +fullname.substr(lastindex, fullname.length());
        }else{
            filename = fullname.substr(0, fullname.length())
            +std::to_string(Utils::s_randomNumber())
            +".log";
        }
        //s_filepath=path;
        s_filepath=filename.c_str();
        s_pOutFileStream = new std::ofstream;
        s_pOutFileStream->open(s_filepath, std::ofstream::out|std::ofstream::app);
        if(!s_pOutFileStream->is_open()){
            std::cerr << "Logfile could not be opened. Logging to stdout" << std::endl;
            s_filepath.erase();
            delete s_pOutFileStream;
        }
        initialized=true;

    s_pOutStream=(s_pOutFileStream!=NULL && s_pOutFileStream->is_open())?s_pOutFileStream:&std::cout;
#if defined _WIN32 || defined _WIN64

	TCHAR szFile[MAX_PATH];
	GetModuleFileName(NULL, szFile, MAX_PATH);
#endif
	*s_pOutStream
		<< " DRILL CLIENT LIBRARY " << std::endl
#if defined _WIN32 || defined _WIN64
		<< " Loaded by process : " << szFile << std::endl
		<< " Current Process Id is: " << ::GetCurrentProcessId() << std::endl
#endif
		<< " Initialized Logging to file (" << path << "). "
		<< std::endl;
	}
}

void Logger::close(){
    //boost::lock_guard<boost::mutex> logLock(Drill::Logger::s_logMutex); 
    boost::lock_guard<boost::mutex> logLock(s_logMutex); 
    if(s_pOutFileStream !=NULL){
        if(s_pOutFileStream->is_open()){
            s_pOutFileStream->close();
        }
        delete s_pOutFileStream; s_pOutFileStream=NULL;
    }
}

// The log call itself cannot be thread safe. Use the DRILL_MT_LOG macro to make 
// this thread safe
std::ostream& Logger::log(logLevel_t level){
    *s_pOutStream << getTime();
    *s_pOutStream << " : "<<levelAsString(level);
    *s_pOutStream << " : "<<getTid();
    *s_pOutStream << " : ";
    return *s_pOutStream;
}


} // namespace Drill

