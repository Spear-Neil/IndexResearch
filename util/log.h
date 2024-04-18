#ifndef UTIL_LOG_H
#define UTIL_LOG_H

#include <stdio.h>
#include <stdlib.h>
#include "macro.h"

/* control the macro definition of LOG/ERROR, CONDITIONAL_LOG/CONDITIONAL_ERROR,
 * before LOG/ERROR print, all graph attributes wound be turned off */

/* CONDITION_ERROR is recommended instead of assert, so even in release mode,
 * you can check whether some undefined behavior happens through ERROR_OPTION */
#ifdef NDEBUG
#define LOG_OPTION   OPTION_OFF // OPTION_ON, OPTION_OFF
#define ERROR_OPTION OPTION_OFF // OPTION_ON, OPTION_OFF
#else
#define LOG_OPTION   OPTION_ON  // OPTION_ON, OPTION_OFF
#define ERROR_OPTION OPTION_ON  // OPTION_ON, OPTION_OFF
#endif

#define NONE_COLOR   GRAPH_ATTR_NONE
#define LOG_COLOR    GRAPH_FONT_YELLOW
#define ERROR_COLOR  GRAPH_FONT_RED

/* the default FILE pointer of LOG/ERROR print */
#define LOG_FILE   stdout
#define ERROR_FILE stdout
#define EXIT_NUM   -2

#define LOG_PRINT(format, ...)   fprintf(LOG_FILE,format, ##__VA_ARGS__)
#define ERROR_PRINT(format, ...) fprintf(ERROR_FILE,format, ##__VA_ARGS__)

#if LOG_OPTION

#define LOG(format, ...)                  \
do {                                      \
LOG_PRINT(NONE_COLOR LOG_COLOR);          \
LOG_PRINT("LOG: file:%s, ", __FILE__);    \
LOG_PRINT("func:%s, ", __FUNCTION__);     \
LOG_PRINT(format, ##__VA_ARGS__);         \
LOG_PRINT(NONE_COLOR"\n");                \
fflush(LOG_FILE);                         \
} while(false)

#else //LOG_OPTION

#define LOG(format, ...) while(false)

#endif //LOG_OPTION

#if ERROR_OPTION

#define ERROR(format, ...)                  \
do {                                        \
ERROR_PRINT(NONE_COLOR ERROR_COLOR);        \
ERROR_PRINT("ERROR: file:%s, ", __FILE__);  \
ERROR_PRINT("line:%i, ", __LINE__);         \
ERROR_PRINT("func:%s, ", __FUNCTION__);     \
ERROR_PRINT(format,  ##__VA_ARGS__);        \
ERROR_PRINT(NONE_COLOR"\n");                \
fflush(ERROR_FILE);                         \
abort();                                    \
} while(false)

#else //ERROR_OPTION

#define ERROR(format, ...) while(false)

#endif //ERROR_OPTION

#define CONDITION_LOG(cond, format, ...) \
do {                                     \
if(cond) LOG(format, ##__VA_ARGS__);     \
} while(false)

#define CONDITION_ERROR(cond, format, ...) \
do {                                       \
if(cond) ERROR(format, ##__VA_ARGS__);     \
} while(false)

#endif //UTIL_LOG_H
