#include <stdio.h>  /* defines FILENAME_MAX */

#include <unistd.h>
#define GetCurrentDir getcwd
#include<iostream>


std::string GetCurrentWorkingDir( void ) {
  char buff[FILENAME_MAX];
  GetCurrentDir( buff, FILENAME_MAX );
  std::string current_working_dir(buff);
  return current_working_dir;
}

std::string pathJoin(const std::string& path1,const std::string& path2){
    if(path1.empty() || path1.back()){
        return path1 + path2;
    }
    else if (path1.back() == '/' || path1.back() == '\\'){
        return path1 + path2;
    }
    else 
        return path1 + "/" + path2;
    
}
