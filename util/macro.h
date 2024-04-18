#ifndef UTIL_MACRO_H
#define UTIL_MACRO_H

/* the macro option status, control macro definition */
#define OPTION_ON 1
#define OPTION_OFF 0


/** some common-used ANSI escape codes,
 * \033[ + function arg + function name */
/* attribute control of print */
#define GRAPH_ATTR_NONE      "\033[0m"
#define GRAPH_ATTR_BOLD      "\033[1m"
#define GRAPH_ATTR_ITALIC    "\033[3m"
#define GRAPH_ATTR_UNDERLINE "\033[4m"

/* font color control of print */
#define GRAPH_FONT_BLACK  "\033[30m"
#define GRAPH_FONT_RED    "\033[31m"
#define GRAPH_FONT_GREEN  "\033[32m"
#define GRAPH_FONT_YELLOW "\033[33m"
#define GRAPH_FONT_BLUE   "\033[34m"
#define GRAPH_FONT_PURPLE "\033[35m"
#define GRAPH_FONT_CYAN   "\033[36m"
#define GRAPH_FONT_WHITE  "\033[37m"

/* background color control of print */
#define GRAPH_BACK_BLACK  "\033[40m"
#define GRAPH_BACK_RED    "\033[41m"
#define GRAPH_BACK_GREEN  "\033[42m"
#define GRAPH_BACK_YELLOW "\033[43m"
#define GRAPH_BACK_BLUE   "\033[44m"
#define GRAPH_BACK_PURPLE "\033[45m"
#define GRAPH_BACK_CYAN   "\033[46m"
#define GRAPH_BACK_WHITE  "\033[47m"


/* macro str cat, #: to_string based on parameter replacement
 * ##: cat two identifier, based on parameter replacement */
#define TO_STRING(str) #str
#define __CAT_IMPL(x, y) X##Y
#define CAT(x, y) __CAT_IMPL(x, y)

/* provide the compiler with branch prediction information */
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x),0)

#define prefetch(line, hint) _mm_prefetch(line, hint)


/* some attributes provided by gcc/g++ */
#ifdef __GNUC__
#define __attribute_impl(attr) __attribute__((attr))
#else
#define __attribute_impl(attr)
#endif

#define __func_attr_impl(attr) __attribute_impl(attr)  // function attributes
#define __type_attr_impl(attr) __attribute_impl(attr)  // type attributes

#define func_noinline __func_attr_impl(__noinline__)
#define func_inline __func_attr_impl(__always_inline__)
#define func_pure __func_attr_impl(__pure__)  // pure function, same arg, same return; no side effects
#define func_const __func_attr_impl(__const__) // more strict than pure function, function is not allowed to read the global memory
#define func_constructor __func_attr_impl(__constructor__) // the function is performed before main func
#define func_destructor __func_attr_impl(__destructor__) // the function is performed after main func
#define func_section(sec_name) __func_attr_impl(__section__(sec_name)) // put the function in the sec_name section
#define func_used  __func_attr_impl(__used__)
#define func_unused __func_attr_impl(__unused__)
#define func_deprecated __func_attr_impl(__deprecated__)
#define func_warn_unused_result __func_attr_impl(__warn_unused_result__) // the func return must be used
#define func_nothrow __func_attr_impl(__nothrow__)

#define type_aligned(align) __type_attr_impl(__aligned__(align))
#define type_packed __type_attr_impl(__packed__)

#endif //UTIL_MACRO_H
