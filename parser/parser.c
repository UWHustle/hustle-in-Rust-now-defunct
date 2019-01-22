/* A Bison parser, made by GNU Bison 3.2.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.2.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 1 "parser.y" /* yacc.c:338  */

#include "parse_node.h"
#include "parser.h"
#include "lexer.h"

void yyerror(parse_node **s, yyscan_t scanner, char const *msg);

#line 77 "parser.c" /* yacc.c:338  */
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 1
#endif

/* In a future release of Bison, this section will be replaced
   by #include "parser.h".  */
#ifndef YY_YY_PARSER_H_INCLUDED
# define YY_YY_PARSER_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif
/* "%code requires" blocks.  */
#line 12 "parser.y" /* yacc.c:353  */

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#line 117 "parser.c" /* yacc.c:353  */

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    TK_ABORT = 258,
    TK_ACTION = 259,
    TK_ADD = 260,
    TK_AFTER = 261,
    TK_ALL = 262,
    TK_ALTER = 263,
    TK_ANALYZE = 264,
    TK_AND = 265,
    TK_ANY = 266,
    TK_AS = 267,
    TK_ASC = 268,
    TK_ATTACH = 269,
    TK_AUTOINCR = 270,
    TK_BEFORE = 271,
    TK_BEGIN = 272,
    TK_BETWEEN = 273,
    TK_BITAND = 274,
    TK_BITNOT = 275,
    TK_BITOR = 276,
    TK_BLOB = 277,
    TK_BY = 278,
    TK_CASCADE = 279,
    TK_CASE = 280,
    TK_CAST = 281,
    TK_CHECK = 282,
    TK_COLLATE = 283,
    TK_COLUMNKW = 284,
    TK_COMMA = 285,
    TK_COMMIT = 286,
    TK_CONCAT = 287,
    TK_CONFLICT = 288,
    TK_CONSTRAINT = 289,
    TK_CREATE = 290,
    TK_CTIME_KW = 291,
    TK_CURRENT = 292,
    TK_DATABASE = 293,
    TK_DEFAULT = 294,
    TK_DEFERRABLE = 295,
    TK_DEFERRED = 296,
    TK_DELETE = 297,
    TK_DESC = 298,
    TK_DETACH = 299,
    TK_DISTINCT = 300,
    TK_DO = 301,
    TK_DOT = 302,
    TK_DROP = 303,
    TK_EACH = 304,
    TK_ELSE = 305,
    TK_END = 306,
    TK_EQ = 307,
    TK_ESCAPE = 308,
    TK_EXCEPT = 309,
    TK_EXCLUSIVE = 310,
    TK_EXISTS = 311,
    TK_EXPLAIN = 312,
    TK_FAIL = 313,
    TK_FILTER = 314,
    TK_FLOAT = 315,
    TK_FOLLOWING = 316,
    TK_FOR = 317,
    TK_FOREIGN = 318,
    TK_FROM = 319,
    TK_GE = 320,
    TK_GROUP = 321,
    TK_GT = 322,
    TK_HAVING = 323,
    TK_ID = 324,
    TK_IF = 325,
    TK_IGNORE = 326,
    TK_IMMEDIATE = 327,
    TK_IN = 328,
    TK_INDEX = 329,
    TK_INDEXED = 330,
    TK_INITIALLY = 331,
    TK_INSERT = 332,
    TK_INSTEAD = 333,
    TK_INTEGER = 334,
    TK_INTERSECT = 335,
    TK_INTO = 336,
    TK_IS = 337,
    TK_ISNULL = 338,
    TK_JOIN = 339,
    TK_JOIN_KW = 340,
    TK_KEY = 341,
    TK_LE = 342,
    TK_LIKE_KW = 343,
    TK_LIMIT = 344,
    TK_LP = 345,
    TK_LSHIFT = 346,
    TK_LT = 347,
    TK_MATCH = 348,
    TK_MINUS = 349,
    TK_NE = 350,
    TK_NO = 351,
    TK_NOT = 352,
    TK_NOTHING = 353,
    TK_NOTNULL = 354,
    TK_NULL = 355,
    TK_OF = 356,
    TK_OFFSET = 357,
    TK_ON = 358,
    TK_OR = 359,
    TK_ORDER = 360,
    TK_OVER = 361,
    TK_PARTITION = 362,
    TK_PLAN = 363,
    TK_PLUS = 364,
    TK_PRAGMA = 365,
    TK_PRECEDING = 366,
    TK_PRIMARY = 367,
    TK_QUERY = 368,
    TK_RAISE = 369,
    TK_RANGE = 370,
    TK_RECURSIVE = 371,
    TK_REFERENCES = 372,
    TK_REINDEX = 373,
    TK_RELEASE = 374,
    TK_REM = 375,
    TK_RENAME = 376,
    TK_REPLACE = 377,
    TK_RESTRICT = 378,
    TK_ROLLBACK = 379,
    TK_ROW = 380,
    TK_ROWS = 381,
    TK_RP = 382,
    TK_RSHIFT = 383,
    TK_SAVEPOINT = 384,
    TK_SELECT = 385,
    TK_SEMI = 386,
    TK_SET = 387,
    TK_SLASH = 388,
    TK_STAR = 389,
    TK_STRING = 390,
    TK_TABLE = 391,
    TK_TEMP = 392,
    TK_THEN = 393,
    TK_TO = 394,
    TK_TRANSACTION = 395,
    TK_TRIGGER = 396,
    TK_UNBOUNDED = 397,
    TK_UNION = 398,
    TK_UNIQUE = 399,
    TK_UPDATE = 400,
    TK_USING = 401,
    TK_VACUUM = 402,
    TK_VALUES = 403,
    TK_VARIABLE = 404,
    TK_VIEW = 405,
    TK_VIRTUAL = 406,
    TK_WHEN = 407,
    TK_WHERE = 408,
    TK_WINDOW = 409,
    TK_WITH = 410,
    TK_WITHOUT = 411
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 29 "parser.y" /* yacc.c:353  */

  int intval;
  char *strval;
  parse_node *node;
  dynamic_array *list;

#line 293 "parser.c" /* yacc.c:353  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (parse_node **parse_tree, yyscan_t scanner);

#endif /* !YY_YY_PARSER_H_INCLUDED  */



#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif


#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYSIZE_T yynewbytes;                                            \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / sizeof (*yyptr);                          \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, (Count) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYSIZE_T yyi;                         \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  70
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   2773

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  157
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  125
/* YYNRULES -- Number of rules.  */
#define YYNRULES  393
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  797

/* YYTRANSLATE[YYX] -- Symbol number corresponding to YYX as returned
   by yylex, with out-of-bounds checking.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   411

#define YYTRANSLATE(YYX)                                                \
  ((unsigned) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, without out-of-bounds checking.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   225,   225,   229,   230,   234,   235,   236,   240,   241,
     245,   249,   250,   251,   252,   253,   254,   255,   256,   257,
     258,   259,   260,   261,   262,   263,   264,   265,   266,   267,
     268,   269,   270,   271,   272,   273,   274,   275,   276,   277,
     278,   279,   280,   281,   282,   283,   284,   285,   286,   289,
     291,   292,   295,   297,   298,   299,   303,   304,   308,   311,
     313,   317,   318,   322,   323,   326,   328,   332,   333,   337,
     341,   342,   343,   346,   348,   349,   350,   354,   355,   359,
     360,   363,   368,   369,   373,   374,   375,   376,   377,   378,
     379,   380,   381,   382,   383,   384,   385,   386,   389,   391,
     394,   396,   400,   401,   402,   403,   407,   408,   409,   410,
     411,   415,   416,   419,   421,   422,   425,   427,   431,   432,
     436,   437,   441,   442,   443,   444,   445,   448,   450,   453,
     455,   458,   460,   464,   465,   466,   470,   471,   475,   476,
     477,   481,   482,   486,   487,   488,   489,   493,   513,   514,
     518,   519,   523,   524,   525,   529,   532,   538,   542,   543,
     547,   548,   549,   553,   554,   560,   561,   565,   571,   572,
     573,   576,   578,   582,   583,   587,   588,   589,   590,   594,
     595,   596,   597,   598,   602,   603,   606,   608,   609,   613,
     614,   617,   619,   623,   624,   628,   629,   630,   634,   635,
     640,   642,   645,   647,   648,   649,   652,   654,   658,   659,
     660,   661,   664,   666,   667,   668,   672,   673,   676,   678,
     682,   683,   687,   688,   689,   693,   694,   695,   696,   697,
     698,   699,   704,   705,   706,   707,   708,   709,   710,   711,
     712,   713,   714,   715,   716,   717,   718,   719,   720,   721,
     722,   723,   724,   725,   726,   727,   728,   729,   730,   731,
     732,   733,   734,   735,   736,   737,   738,   739,   740,   741,
     742,   743,   744,   748,   749,   750,   751,   752,   753,   757,
     758,   759,   760,   764,   765,   769,   770,   774,   775,   779,
     780,   784,   785,   789,   792,   798,   802,   808,   810,   814,
     815,   818,   820,   824,   825,   828,   830,   834,   835,   836,
     837,   838,   842,   843,   847,   851,   855,   856,   857,   858,
     862,   863,   864,   865,   868,   870,   873,   875,   879,   880,
     884,   885,   888,   890,   891,   895,   896,   897,   898,   902,
     903,   904,   907,   909,   913,   914,   918,   921,   923,   927,
     931,   932,   935,   937,   941,   942,   946,   949,   951,   952,
     955,   957,   958,   962,   963,   967,   968,   972,   976,   980,
     981,   984,   986,   987,   991,   992,   996,   997,  1001,  1002,
    1006,  1007,  1008,  1012,  1016,  1017,  1020,  1022,  1026,  1027,
    1031,  1032,  1036,  1037
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 1
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "ABORT", "ACTION", "ADD", "AFTER", "ALL",
  "ALTER", "ANALYZE", "AND", "ANY", "AS", "ASC", "ATTACH", "AUTOINCR",
  "BEFORE", "BEGIN", "BETWEEN", "BITAND", "BITNOT", "BITOR", "BLOB", "BY",
  "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMNKW", "COMMA",
  "COMMIT", "CONCAT", "CONFLICT", "CONSTRAINT", "CREATE", "CTIME_KW",
  "CURRENT", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE",
  "DESC", "DETACH", "DISTINCT", "DO", "DOT", "DROP", "EACH", "ELSE", "END",
  "EQ", "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL",
  "FILTER", "FLOAT", "FOLLOWING", "FOR", "FOREIGN", "FROM", "GE", "GROUP",
  "GT", "HAVING", "ID", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX",
  "INDEXED", "INITIALLY", "INSERT", "INSTEAD", "INTEGER", "INTERSECT",
  "INTO", "IS", "ISNULL", "JOIN", "JOIN_KW", "KEY", "LE", "LIKE_KW",
  "LIMIT", "LP", "LSHIFT", "LT", "MATCH", "MINUS", "NE", "NO", "NOT",
  "NOTHING", "NOTNULL", "NULL", "OF", "OFFSET", "ON", "OR", "ORDER",
  "OVER", "PARTITION", "PLAN", "PLUS", "PRAGMA", "PRECEDING", "PRIMARY",
  "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REINDEX",
  "RELEASE", "REM", "RENAME", "REPLACE", "RESTRICT", "ROLLBACK", "ROW",
  "ROWS", "RP", "RSHIFT", "SAVEPOINT", "SELECT", "SEMI", "SET", "SLASH",
  "STAR", "STRING", "TABLE", "TEMP", "THEN", "TO", "TRANSACTION",
  "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM",
  "VALUES", "VARIABLE", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW",
  "WITH", "WITHOUT", "$accept", "input", "cmdlist", "ecmd", "explain",
  "cmdx", "cmd", "trans_opt", "transtype", "savepoint_opt", "createkw",
  "ifnotexists", "temp", "create_table_args", "table_options",
  "columnlist", "columnname", "nm", "typetoken", "typename", "signed",
  "scanpt", "carglist", "ccons", "autoinc", "refargs", "refarg", "refact",
  "defer_subclause", "init_deferred_pred_opt", "conslist_opt", "conslist",
  "tconscomma", "tcons", "defer_subclause_opt", "onconf", "orconf",
  "resolvetype", "ifexists", "select", "selectnowith", "multiselect_op",
  "oneselect", "values", "distinct", "sclp", "selcollist", "as", "from",
  "stl_prefix", "seltablist", "dbnm", "fullname", "xfullname", "joinop",
  "on_opt", "indexed_opt", "using_opt", "orderby_opt", "sortlist",
  "sortorder", "groupby_opt", "having_opt", "limit_opt", "where_opt",
  "setlist", "upsert", "insert_cmd", "idlist_opt", "idlist", "expr",
  "term", "likeop", "between_op", "in_op", "case_exprlist", "case_else",
  "case_operand", "exprlist", "nexprlist", "paren_exprlist", "uniqueflag",
  "eidlist_opt", "eidlist", "collate", "nmnum", "plus_num", "minus_num",
  "trigger_decl", "trigger_time", "trigger_event", "foreach_clause",
  "when_clause", "trigger_cmd_list", "trnm", "tridxby", "trigger_cmd",
  "raisetype", "key_opt", "database_kw_opt", "add_column_fullname",
  "kwcolumn_opt", "create_vtab", "vtabarglist", "vtabarg", "vtabargtoken",
  "lp", "anylist", "with", "wqlist", "windowdefn_list", "windowdefn",
  "window", "part_opt", "frame_opt", "range_or_rows", "frame_bound_s",
  "frame_bound_e", "frame_bound", "window_clause", "over_clause",
  "filter_opt", "id", "ids", "number", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   338,   339,   340,   341,   342,   343,   344,
     345,   346,   347,   348,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,   359,   360,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,   391,   392,   393,   394,
     395,   396,   397,   398,   399,   400,   401,   402,   403,   404,
     405,   406,   407,   408,   409,   410,   411
};
# endif

#define YYPACT_NINF -568

#define yypact_value_is_default(Yystate) \
  (!!((Yystate) == (-568)))

#define YYTABLE_NINF -387

#define yytable_value_is_error(Yytable_value) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     617,   -57,   634,   220,    27,   105,  -568,   220,   225,   105,
      19,   634,   634,   135,   105,   634,   356,  -568,   634,   182,
     314,   286,   316,  -568,   837,   169,  -568,   160,  -568,   -16,
    -568,   296,   262,    51,   634,  -568,  -568,  -568,  -568,   311,
    -568,  -568,  1337,  -568,  -568,  -568,   105,   634,  -568,  1337,
     295,   295,   295,   295,  -568,   271,   311,   311,  -568,   634,
     269,  -568,  -568,  -568,  -568,  -568,  1337,   634,   335,    -1,
    -568,  -568,   284,  -568,  -568,  -568,   297,   354,   357,   435,
    -568,  -568,   452,   147,   376,  -568,   413,   375,  -568,   375,
     400,   444,   377,   494,   634,  -568,  1337,  -568,  1337,   412,
    -568,   415,  -568,  -568,   464,  1000,  1337,  1337,  -568,  1337,
     430,   475,  -568,   476,  1768,  -568,   227,  -568,  -568,  2430,
     468,   634,   634,   634,   634,  -568,   324,  -568,  -568,   135,
    -568,   152,  2430,    68,    -1,   634,   513,   634,   -16,  -568,
     459,   459,   459,   459,   459,    10,  -568,  -568,  1337,    71,
      94,   634,    45,  -568,   634,   634,   634,     4,   501,  -568,
    -568,  2430,   380,  1337,   256,   325,   406,  1826,   504,  -568,
    2581,  -568,    44,   634,  1337,  1337,  -568,  1337,  1337,   -18,
    1337,  1337,  1337,  1337,  -568,  1337,  -568,  1337,  -568,  1337,
    1337,  -568,  1337,  1337,   305,  -568,  1337,  1337,  1337,  1337,
    1337,  1337,  1337,  1337,   431,    24,  -568,  -568,  -568,  -568,
    -568,   564,   564,   634,   844,  -568,  -568,   386,  1337,  -568,
     -16,   512,    74,   451,   335,   445,   634,   634,   634,   634,
     634,   481,   375,   273,    -7,   420,   185,  -568,  -568,  -568,
    -568,  -568,  -568,   259,   -19,  -568,  -568,  -568,  -568,  -568,
    -568,  -568,   -19,   466,  -568,  -568,   634,   634,   634,  1337,
      -4,  1919,   432,   634,    -1,  -568,  -568,  1337,   433,   528,
     534,  2581,  1977,   248,   248,  -568,  -568,  -568,   547,   588,
     159,   159,   588,   159,   248,   159,    86,   588,  -568,  -568,
    -568,  -568,  -568,  2523,    86,   381,   248,   381,   381,  2639,
     588,  1000,   311,   455,  1337,  -568,  -568,  -568,  -568,    -5,
    -568,    -5,  -568,  -568,  -568,  -568,  -568,   465,   486,  -568,
    -568,   544,  2430,   462,   261,  1337,   527,  2430,   -18,   260,
     634,  -568,   256,   584,   546,   311,   311,   311,   311,   311,
     634,   634,  -568,   536,  -568,   477,  -568,  -568,    94,    50,
     634,   634,   596,   554,   386,   489,   634,    42,  -568,   483,
    -568,   -18,  1371,  1337,  1337,   579,   -18,  -568,    -1,  2070,
    -568,   634,   634,  1337,  -568,  1337,   505,   508,   606,   548,
     250,   510,  -568,  -568,  -568,  -568,   393,    25,   256,   311,
    -568,  -568,   469,  -568,  2430,   619,   572,  -568,  -568,  -568,
    -568,   512,   518,   557,  -568,   516,    41,   107,   335,   553,
     604,    -8,    -8,  -568,   634,  -568,  -568,  -568,  -568,  -568,
     647,   634,  -568,   558,   488,  -568,   188,   521,   561,   634,
     280,  -568,   167,  -568,  1337,  2430,  1429,  -568,   539,  -568,
     545,  -568,  2430,   588,  -568,  -568,  1337,  -568,   580,  -568,
     570,   250,  -568,   634,  -568,  -568,   556,   104,    22,  -568,
     543,  1337,  1337,   -43,   260,  -568,   256,   634,   256,   634,
    -568,  -568,  -568,   583,     0,   669,   634,   634,   662,   611,
     386,   562,   466,    52,   634,  -568,   666,   601,   634,   641,
       9,   634,  -568,  -568,   663,  -568,  -568,   610,   -18,   634,
     -36,   622,   222,   598,   616,   634,   598,  -568,  -568,   360,
    -568,  2430,  1337,  -568,  -568,   578,   565,   525,  -568,  -568,
      25,    25,  1337,   -19,  -568,   623,   606,  2430,   634,   601,
     558,  -568,   597,  -568,  -568,   680,  -568,  -568,  -568,  -568,
     624,   625,   256,   636,  -568,   634,  -568,  -568,   488,   256,
    -568,  -568,  1337,  1337,  -568,   200,  1337,   602,   558,  -568,
     206,  1337,  -568,  -568,  1337,   253,   253,   401,     8,  -568,
     622,   598,   696,  -568,   260,   335,  -568,   202,  -568,  -568,
    2430,  -568,  1337,   626,  -568,  -568,   627,   627,   605,   627,
    -568,   719,   704,  -568,  -568,   601,  -568,   166,   612,   280,
     634,   634,  -568,  1337,  -568,  -568,     9,   561,   708,  1522,
    1615,   688,  2430,   634,   690,   601,   645,  1337,  2128,  2221,
    -568,  -568,  -568,  -568,  -568,  -568,  -568,  -568,  -568,    45,
     598,  -568,   360,  -568,  2279,   721,   558,  1337,   599,   599,
      25,   599,   659,   634,  -568,   664,   634,   667,   670,   665,
    -568,   207,  -568,   603,   722,   695,   209,  -568,  -568,  1337,
    -568,  1337,  1337,  1337,   210,  1337,  -568,  -568,   213,  -568,
    -568,  -568,  -568,  -568,  -568,   743,   313,   639,  -568,  1337,
     292,  2430,   671,  -568,  -568,   627,  -568,  -568,  -568,  1337,
    -568,   673,   677,  1337,   280,  -568,    32,   634,  -568,   724,
     618,   386,  -568,  -568,  1522,  2430,  2430,  2430,   716,  2430,
     386,  -568,  -568,   634,    30,  -568,  -568,   606,  -568,  -568,
     648,  1106,   634,   599,  2372,   634,  1337,   214,  -568,  -568,
     649,  1337,  -568,  -568,  -568,  1337,   730,  -568,    85,    85,
      85,  -568,  1206,   652,   676,  1673,  -568,  -568,   216,  -568,
     598,   217,   402,   598,  -568,  2430,  2430,   257,  -568,   774,
    -568,    36,  -568,  -568,  -568,   770,  -568,  -568,  -568,  -568,
    -568,  -568,   675,   657,  -568,  -568,   681,  -568,  -568,  -568,
    1252,   634,   598,   488,   733,  -568,  -568,   335,  -568,     9,
    -568,  -568,  -568,     3,   756,  -568,  -568
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
     360,     0,    42,   345,    52,    49,    58,   345,     0,    49,
       8,     0,    40,    57,    49,     0,   154,     5,    29,     0,
       0,     0,   360,     4,   360,     0,    10,    62,    22,   140,
     141,   149,    47,     0,     0,   388,   389,    72,    71,   171,
      70,   344,     0,    53,    55,    54,    49,    50,    12,     0,
     137,   137,   137,   137,    13,     0,   171,   171,    56,     0,
      14,    15,   153,   152,   156,    30,     0,     0,   301,   361,
       1,     3,     0,     6,    61,   299,     0,     0,     0,     0,
     145,   146,   143,     0,     0,   352,     0,   131,   217,   131,
       0,   173,   346,     0,     0,    43,     0,   275,   292,     0,
     278,     0,   274,   277,   225,     0,     0,     0,   273,     0,
       0,   276,   228,     0,     0,   222,   224,    11,    51,    39,
       0,     0,     0,     0,     0,     9,    31,    41,    16,    57,
      81,   163,   296,     0,   362,     0,     0,     0,   138,     7,
      59,    59,    59,    59,    59,    81,   144,   142,     0,     0,
     350,     0,     0,   216,     0,     0,     0,   347,   347,   172,
     261,   291,     0,     0,     0,     0,     0,   296,     0,   263,
     260,   262,     0,     0,     0,     0,   283,     0,     0,     0,
       0,     0,     0,     0,   285,     0,   256,     0,   279,     0,
       0,   280,     0,     0,     0,   257,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   154,   136,    28,    19,    37,
      21,     0,     0,     0,     0,   155,   166,   206,     0,   150,
     139,   305,     0,     0,   301,     0,     0,     0,     0,     0,
       0,     0,   131,     0,    81,     0,     0,   352,    48,   354,
     356,   353,   357,   175,   186,   340,   341,   134,   135,   339,
     132,   133,   186,   218,   174,   348,     0,     0,     0,     0,
     290,     0,     0,     0,     0,   266,   223,     0,     0,     0,
     226,   236,   342,   244,   245,   390,   391,   229,   253,   242,
     240,   239,   259,   241,   246,   238,   249,   243,   284,   286,
     281,   282,   258,   237,   248,   252,   247,   251,   250,   254,
     264,   294,   171,     0,   294,   311,   310,   393,   392,     0,
     309,     0,   308,    32,   307,    34,   313,     0,     0,    17,
     158,     0,    81,     0,   164,     0,   198,   295,     0,   197,
       0,   302,     0,     0,     0,   171,   171,   171,   171,   171,
       0,     0,    81,     0,    36,     0,   329,   151,   351,     0,
       0,     0,     0,     0,   206,     0,     0,     0,    44,     0,
      83,    73,     0,     0,     0,     0,    73,   269,     0,   295,
     271,     0,     0,     0,    38,     0,     0,     0,   293,   297,
     232,     0,   314,   312,    33,    35,     0,   162,   166,   171,
     179,   180,     0,   165,   207,     0,   200,   306,   195,   196,
     304,   305,     0,     0,    60,     0,     0,   319,   301,     0,
     330,   332,   332,   338,     0,   328,   359,   357,   355,   178,
     176,     0,   188,   191,     0,   221,     0,     0,   212,     0,
      45,    69,    74,    77,     0,   289,     0,   270,     0,   235,
       0,   227,   343,   255,   267,   265,   294,   268,     0,   234,
       0,   231,   159,     0,   157,   161,     0,     0,   162,   181,
       0,     0,     0,   191,   197,   363,     0,     0,     0,     0,
      18,   317,   316,     0,     0,     0,     0,     0,     0,     0,
     206,     0,   218,     0,     0,   187,     0,   202,     0,     0,
     206,     0,   219,    26,     0,    25,    46,     0,     0,     0,
      81,   113,     0,   129,     0,     0,   129,    82,    96,     0,
      78,   288,     0,   230,   272,     0,     0,     0,   233,   160,
     162,   162,   294,   186,   182,     0,   199,   201,     0,   202,
     191,   303,     0,   349,    64,   116,    83,   318,   320,   321,
     322,     0,     0,     0,   331,     0,   334,    81,     0,     0,
     358,   177,     0,     0,    23,     0,     0,     0,   191,   220,
       0,     0,    97,    84,     0,     0,     0,     0,     0,   112,
     113,   129,     0,    90,   197,   301,    93,     0,    79,    80,
     287,   298,     0,   370,   385,   384,   185,   185,     0,   185,
     183,     0,   383,   365,   147,   202,   364,     0,     0,    68,
       0,     0,    20,     0,   333,   337,   206,   212,   192,   197,
     203,     0,   210,     0,     0,   202,     0,     0,     0,     0,
     276,    81,    81,    81,    89,   114,   115,   111,    91,     0,
     129,   100,     0,    75,     0,     0,   191,     0,   190,   190,
     162,   190,     0,     0,   148,     0,     0,     0,     0,     0,
      83,   121,   119,    65,   323,   324,     0,    81,    81,     0,
     194,     0,     0,     0,     0,     0,    24,   215,     0,    94,
      86,    88,    87,    85,   130,    98,    95,     0,   387,     0,
     371,   184,     0,   169,   170,   185,   167,   367,   366,     0,
     122,     0,     0,     0,    67,   120,     0,     0,    63,     0,
     326,   206,   335,   336,   197,   205,   204,   211,     0,   208,
     206,    99,    92,     0,     0,   101,    76,   369,   374,   375,
       0,     0,     0,   190,     0,     0,     0,     0,   118,    66,
       0,     0,   315,    27,   193,     0,     0,   102,     0,     0,
       0,   368,     0,     0,     0,     0,   372,   376,     0,   168,
     129,     0,    98,   129,   325,   327,   209,     0,   108,     0,
     109,     0,   104,   103,   105,     0,   381,   377,   382,   380,
     189,   125,     0,     0,   124,   214,     0,   110,   107,   106,
       0,     0,   129,     0,     0,   373,   378,   301,   123,   206,
     379,   100,   213,   127,     0,   128,   126
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -568,  -568,  -568,   777,  -568,   778,  -568,    62,  -568,   674,
    -568,   366,  -568,  -568,  -568,  -568,  -441,    -2,   446,  -568,
     183,  -109,  -476,  -568,    64,    23,  -568,  -283,    26,   247,
    -568,  -568,  -568,   122,  -568,  -484,   -62,   191,   391,   -94,
     -50,  -568,   740,  -568,   620,  -568,  -568,  -435,  -568,  -568,
     436,   -54,  -117,   318,  -568,  -499,  -235,  -418,  -433,  -567,
    -444,  -568,  -568,  -325,  -339,  -534,   221,   594,   347,  -463,
     608,   -48,  -568,  -568,  -568,  -568,  -568,  -568,  -280,   -65,
    -568,  -568,  -216,   106,   429,   628,  -468,   276,  -568,  -568,
    -568,  -568,  -568,  -568,  -231,   422,   607,   660,  -568,   828,
    -568,   678,  -568,  -568,   600,  -568,  -568,   421,  -568,   -41,
    -568,   196,   201,  -568,  -568,  -568,   100,  -568,    67,  -568,
     399,  -568,   387,  -167,    76
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    21,    22,    23,    24,    25,    26,    48,    46,    59,
      27,   226,    77,   470,   698,   535,   360,   113,   431,   432,
     577,   233,   430,   507,   712,   676,   715,   762,   508,   569,
     598,   651,   696,   652,   796,   573,   153,   250,   121,    28,
      29,    83,    30,    31,    64,   130,   131,   454,   217,   323,
     324,    95,    92,   244,   393,   638,   354,   683,   487,   608,
     400,   396,   463,   554,   326,   490,   495,    90,   357,   426,
     132,   115,   202,   203,   204,   260,   365,   162,   377,   378,
     447,    78,   136,   222,   329,   313,   314,   579,    79,   474,
     541,   700,   732,   234,   411,   480,   235,   251,   374,    42,
      93,   257,    32,   149,   150,   241,   242,   349,    33,    69,
     592,   593,   585,   636,   720,   721,   746,   785,   747,   530,
     449,   450,    40,   455,   316
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      39,   133,   126,   127,   207,   208,   209,   210,   333,    56,
      57,   166,   277,    61,   606,   423,    65,   355,    68,   138,
     531,   214,   576,   523,   381,   555,   134,   154,   536,   137,
     529,    62,    91,   255,   453,   231,   656,   453,    80,   557,
     168,   578,   538,   501,   344,   118,   363,   245,   245,   625,
     668,   275,   231,   468,   564,   307,   352,   128,   565,   645,
     599,   416,   486,   416,    81,    68,   646,   478,    43,    63,
     262,    54,   738,   566,   308,   778,    60,   539,   353,    34,
     626,   427,    44,   236,   220,   586,   587,   628,   639,   479,
     641,   275,   159,    86,   275,   647,   713,   595,   218,    45,
     794,   237,   246,   246,   330,   239,   714,   739,   117,   758,
     412,   528,   522,   471,   179,   268,   247,   276,   180,    91,
      91,    91,    91,   472,   264,   615,   727,    82,    87,    16,
     630,   469,    55,   221,   390,   224,   779,   654,   232,   342,
     417,   547,   417,   256,   648,   540,   675,    19,   364,   243,
     664,   558,   243,   243,   254,   232,   650,   276,   303,   752,
     276,   397,   325,    68,   578,   660,   515,   248,   249,   249,
     341,   270,    16,    88,   694,   740,   649,   418,   177,   550,
     178,   759,   215,   482,   240,   473,   723,   179,   391,   392,
      19,   180,   475,   645,   433,   219,    89,   165,   238,   433,
     646,   331,   302,   680,   594,   685,   198,   376,   760,   312,
     312,   319,   321,   387,   138,   218,   216,   761,   491,   200,
     201,   684,   368,   686,   335,   336,   337,   338,   339,   647,
     491,   521,   632,   413,  -300,    35,   275,   695,   402,   659,
     491,    36,   588,   659,   659,    47,   491,   330,   379,   789,
     189,    37,   616,   192,   358,   359,   361,   509,    41,   748,
     734,    68,   570,   428,    58,   510,   771,   657,   197,   774,
     644,   350,    66,   398,   -70,    97,   179,    16,   648,   198,
     180,   405,   406,   407,   408,   409,    70,   199,   589,   100,
     666,   390,   200,   201,   456,    19,   617,    74,   788,    50,
      73,    38,   276,   399,    75,   749,   351,   497,   498,   448,
     649,    76,   347,   102,   499,   492,    -2,   205,   220,   500,
     501,   389,   571,   288,     1,     2,    84,   611,   401,   633,
       3,   562,   103,     4,  -117,   458,   701,   708,   410,   410,
     710,   753,   192,   770,   772,   391,   392,     5,   419,   420,
      87,     6,    85,   108,   425,   775,  -386,   197,    94,   631,
       7,    51,   733,    62,     8,   120,    52,     9,   198,   440,
     441,   736,   532,    10,   534,    53,   211,   502,   289,   125,
     503,   200,   201,    35,   270,   382,    16,   383,   620,    36,
     460,   567,   504,   290,    35,    88,   526,   505,   291,    37,
      36,    63,   776,    16,    19,   292,   713,   718,   129,   179,
      37,   165,   410,   180,   212,   139,   714,   711,   719,   485,
     307,    19,   489,    97,   506,   135,    11,   496,   165,   116,
      67,   144,   659,   140,    12,    13,   116,   100,   605,   308,
      14,   263,   122,   123,   124,    15,    16,    17,   602,    38,
     792,   519,   145,   116,   309,   607,   763,   764,   525,   146,
      38,   102,    35,    18,    19,   533,   148,   361,    36,   311,
      35,    20,   252,   253,   543,   544,    36,   151,    37,   152,
     103,   155,   551,   116,   655,   116,   425,   315,   318,   559,
     141,   156,   116,   116,   116,   142,   116,   563,   157,   158,
      35,   108,   163,   575,   143,   164,    36,   227,   228,   229,
     230,   -72,   671,   672,   673,   584,    37,   621,   622,   623,
     172,   301,   -71,   173,   206,   223,   591,   452,    38,   225,
     255,    35,   259,   265,   267,   116,   620,    36,    35,   325,
     328,   332,   334,   604,    36,   340,   489,    37,   702,   703,
     116,   346,   388,   459,    37,   614,   356,    35,   371,   367,
     370,   116,   116,    36,   116,   116,    38,   116,   116,   116,
     116,   791,   116,    37,   116,   179,   116,   116,   488,   116,
     116,   372,   380,   116,   116,   116,   116,   116,   116,   116,
     116,   386,   384,   395,    35,   361,   403,    38,   425,    91,
      36,   116,   404,   305,    38,   116,   306,   177,   415,   178,
      37,   425,    35,   385,   717,   583,   179,   414,    36,   421,
     180,   424,   429,    38,   307,     1,     2,   524,    37,   422,
     437,     3,   444,    35,     4,   445,   218,   451,   446,    36,
     462,   591,   461,   308,   690,   465,   116,   466,     5,    37,
     114,   477,     6,   182,   116,   183,   476,   119,   309,   484,
      38,     7,   467,   486,   494,     8,   513,   310,     9,   493,
     516,    35,   514,   311,    10,   187,   517,    36,    38,   189,
     190,   542,   192,   520,   537,   545,   546,    37,   116,   552,
     553,   116,   613,   556,   548,   729,   560,   197,   568,    38,
     561,   572,   574,    35,   160,   581,   161,   590,   198,    36,
     597,   737,   116,   167,   169,   170,   199,   171,   582,    37,
     425,   200,   201,   221,   596,   600,   603,    11,   601,   629,
     637,   642,   640,   635,   643,    12,    13,    38,   659,   653,
     663,    14,   665,   667,   679,   682,    15,    16,    17,   583,
     116,   116,   491,   691,   689,   693,   692,   699,   711,   697,
     116,   722,   116,   725,    18,    19,   716,   726,   735,    38,
     731,   261,    20,   730,   754,   741,   757,   766,   777,   787,
     780,   489,   271,   272,   782,   273,   274,   767,   278,   279,
     280,   281,   781,   282,   790,   283,   570,   284,   285,    71,
     286,   287,    72,   213,   293,   294,   295,   296,   297,   298,
     299,   300,   438,   783,   793,   677,   773,   627,   728,   795,
     674,   116,   322,   147,   457,   304,   327,   343,   658,   549,
     464,   751,   269,   116,   481,    49,   258,   348,   483,   688,
     317,   345,   765,   687,     0,     1,     2,   786,   116,   116,
     518,     3,     0,     0,     4,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    96,     0,    97,   362,     5,    98,
      99,     0,     6,     0,     0,   369,     0,     0,     0,     0,
     100,     7,     0,     0,     0,     8,     0,     0,     9,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   116,
     101,     0,     0,     0,   102,     0,     0,     0,     0,   116,
       0,     0,     0,    35,     0,     0,     0,     0,     0,    36,
       0,     0,     0,   103,     0,     0,     0,     0,     0,   104,
       0,     0,     0,   394,   105,     0,     0,     0,   106,   116,
     116,   107,     0,   116,   108,     0,     0,    11,   116,     0,
       0,   116,     0,   109,   624,    12,    13,     0,   110,     0,
       0,    14,     0,     0,     0,     0,    15,    16,     0,   116,
       0,   435,   436,     0,     0,     0,     0,     0,   320,   111,
       0,   442,     0,   443,    18,    19,     0,     0,     0,     0,
     116,     0,    20,   112,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   116,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      96,     0,    97,     0,   116,    98,    99,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   100,     0,     0,     0,
       0,     0,   511,     0,     0,     0,   116,     0,   116,   116,
     116,     0,   116,     0,     0,     0,   101,     0,     0,     0,
     102,     0,     0,     0,     0,     0,   116,     0,     0,    35,
     527,     0,     0,     0,     0,    36,   116,     0,     0,   103,
     116,     0,     0,     0,     0,   104,     0,     0,     0,     0,
     105,     0,     0,     0,   106,     0,     0,   107,     0,     0,
     108,     0,     0,     0,     0,     0,     0,     0,   116,   109,
       0,     0,     0,   116,   110,     0,     0,     0,   116,     0,
     580,     0,   116,     0,   742,     0,    96,     0,    97,   116,
      16,    98,    99,     0,     0,   111,     0,     0,     0,     0,
       0,     0,   100,   743,     0,     0,     0,     0,    19,   112,
       0,     0,     0,     0,     0,   165,     0,     0,     0,     0,
     609,   610,   101,     0,   612,     0,   102,   116,     0,   618,
       0,     0,   619,     0,     0,    35,     0,     0,     0,     0,
       0,    36,     0,     0,     0,   103,     0,     0,     0,     0,
     634,   104,     0,     0,     0,     0,   105,     0,     0,     0,
     106,     0,     0,   107,     0,     0,   108,     0,     0,     0,
       0,   609,     0,     0,     0,   109,     0,     0,     0,     0,
     110,     0,     0,     0,     0,   609,    96,     0,    97,     0,
       0,    98,    99,     0,     0,     0,     0,     0,     0,     0,
       0,   111,   100,   743,     0,   681,     0,     0,   744,     0,
       0,     0,     0,     0,     0,   112,     0,     0,     0,     0,
       0,     0,   101,     0,     0,     0,   102,   704,     0,   705,
     706,   707,    96,   709,    97,    35,     0,    98,    99,     0,
       0,    36,     0,     0,     0,   103,     0,     0,   100,   743,
       0,   104,     0,     0,     0,     0,   105,   724,     0,     0,
     106,   609,     0,   107,     0,     0,   108,     0,   101,     0,
       0,     0,   102,     0,     0,   109,     0,     0,     0,     0,
     110,    35,     0,     0,     0,     0,     0,    36,     0,   745,
       0,   103,     0,     0,   609,     0,     0,   104,     0,   755,
       0,   111,   105,   756,     0,     0,   106,     0,   744,   107,
     745,     0,   108,     0,     0,   112,     0,    96,     0,    97,
       0,   109,    98,    99,     0,     0,   110,     0,     0,     0,
       0,     0,     0,   100,     0,     0,     0,     0,     0,     0,
       0,   174,     0,     0,     0,     0,     0,   111,   745,   176,
     177,     0,   178,   101,   784,     0,     0,   102,     0,   179,
       0,   112,     0,   180,     0,     0,    35,     0,     0,     0,
       0,     0,    36,     0,     0,     0,   103,     0,     0,     0,
       0,     0,   104,   181,     0,     0,     0,   105,     0,     0,
       0,   106,     0,     0,   107,     0,   182,   108,   183,   174,
       0,     0,     0,     0,   184,     0,   109,   176,   177,     0,
     178,   110,     0,   185,   186,     0,     0,   179,   187,   188,
       0,   180,   189,   190,   191,   192,   193,     0,   194,     0,
     195,     0,   111,     0,     0,   196,     0,     0,     0,     0,
     197,   181,     0,     0,     0,     0,   112,     0,     0,     0,
       0,   198,     0,     0,   182,     0,   183,     0,     0,   199,
       0,     0,   184,     0,   200,   201,     0,     0,     0,   434,
       0,   185,   186,     0,     0,     0,   187,   188,     0,     0,
     189,   190,   191,   192,   193,     0,   194,     0,   195,     0,
       0,     0,   174,   196,     0,   398,     0,     0,   197,     0,
     176,   177,     0,   178,     0,     0,     0,     0,     0,   198,
     179,     0,     0,     0,   180,     0,     0,   199,     0,     0,
       0,     0,   200,   201,     0,   399,     0,   512,     0,     0,
       0,     0,     0,     0,   181,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   182,     0,   183,
       0,     0,     0,     0,     0,   184,     0,     0,     0,     0,
       0,     0,     0,     0,   185,   186,     0,     0,     0,   187,
     188,     0,     0,   189,   190,   191,   192,   193,     0,   194,
       0,   195,     0,     0,     0,   174,   196,     0,     0,     0,
       0,   197,     0,   176,   177,     0,   178,     0,     0,     0,
       0,     0,   198,   179,     0,   661,     0,   180,     0,     0,
     199,     0,     0,     0,     0,   200,   201,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   181,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     182,     0,   183,   174,     0,     0,     0,     0,   184,     0,
       0,   176,   177,     0,   178,     0,     0,   185,   186,     0,
       0,   179,   187,   188,     0,   180,   189,   190,   191,   192,
     193,     0,   194,     0,   195,     0,     0,   662,     0,   196,
       0,     0,     0,     0,   197,   181,     0,     0,     0,     0,
       0,     0,     0,     0,   768,   198,     0,     0,   182,     0,
     183,     0,     0,   199,     0,     0,   184,     0,   200,   201,
       0,     0,     0,     0,     0,   185,   186,     0,     0,     0,
     187,   188,     0,     0,   189,   190,   191,   192,   193,     0,
     194,     0,   195,     0,     0,     0,     0,   196,   174,     0,
     175,     0,   197,     0,   769,     0,   176,   177,     0,   178,
       0,     0,     0,   198,     0,     0,   179,     0,     0,     0,
     180,   199,     0,     0,     0,     0,   200,   201,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     181,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   182,     0,   183,   174,     0,     0,     0,
       0,   184,     0,     0,   176,   177,     0,   178,     0,     0,
     185,   186,     0,     0,   179,   187,   188,     0,   180,   189,
     190,   191,   192,   193,     0,   194,     0,   195,     0,     0,
       0,     0,   196,     0,     0,     0,     0,   197,   181,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   198,     0,
       0,   182,     0,   183,     0,     0,   199,     0,     0,   184,
       0,   200,   201,     0,     0,     0,     0,     0,   185,   186,
       0,     0,     0,   187,   188,     0,     0,   189,   190,   191,
     192,   193,     0,   194,     0,   195,     0,     0,     0,   174,
     196,   366,     0,     0,     0,   197,     0,   176,   177,     0,
     178,     0,     0,     0,     0,     0,   198,   179,     0,     0,
       0,   180,     0,   266,   199,     0,     0,     0,     0,   200,
     201,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   181,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   182,     0,   183,   174,     0,     0,
       0,     0,   184,     0,     0,   176,   177,     0,   178,     0,
       0,   185,   186,     0,     0,   179,   187,   188,     0,   180,
     189,   190,   191,   192,   193,     0,   194,     0,   195,     0,
       0,     0,     0,   196,     0,     0,     0,     0,   197,   181,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   198,
       0,     0,   182,     0,   183,     0,     0,   199,     0,     0,
     184,     0,   200,   201,     0,     0,     0,     0,     0,   185,
     186,     0,     0,   373,   187,   188,     0,     0,   189,   190,
     191,   192,   193,     0,   194,     0,   195,     0,     0,     0,
     174,   196,     0,     0,     0,     0,   197,     0,   176,   177,
       0,   178,     0,     0,     0,     0,     0,   198,   179,     0,
       0,     0,   180,     0,     0,   199,     0,     0,     0,     0,
     200,   201,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   181,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   182,     0,   183,   174,     0,
       0,     0,     0,   184,     0,     0,   176,   177,     0,   178,
       0,     0,   185,   186,     0,     0,   179,   187,   188,     0,
     180,   189,   190,   191,   192,   193,     0,   194,     0,   195,
       0,     0,     0,     0,   196,     0,     0,     0,     0,   197,
     181,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     198,     0,     0,   182,     0,   183,     0,   439,   199,     0,
       0,   184,     0,   200,   201,     0,     0,     0,     0,     0,
     185,   186,     0,     0,     0,   187,   188,     0,     0,   189,
     190,   191,   192,   193,     0,   194,     0,   195,     0,     0,
       0,   174,   196,     0,     0,     0,     0,   197,     0,   176,
     177,     0,   178,     0,     0,     0,     0,     0,   198,   179,
       0,     0,     0,   180,     0,   669,   199,     0,     0,     0,
       0,   200,   201,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   181,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   182,     0,   183,   174,
       0,     0,     0,     0,   184,     0,     0,   176,   177,     0,
     178,     0,     0,   185,   186,     0,     0,   179,   187,   188,
       0,   180,   189,   190,   191,   192,   193,     0,   194,     0,
     195,     0,     0,     0,     0,   196,     0,     0,     0,     0,
     197,   181,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   198,     0,     0,   182,     0,   183,     0,   670,   199,
       0,     0,   184,     0,   200,   201,     0,     0,     0,     0,
       0,   185,   186,     0,     0,     0,   187,   188,     0,     0,
     189,   190,   191,   192,   193,     0,   194,     0,   195,     0,
       0,     0,   174,   196,     0,     0,     0,     0,   197,     0,
     176,   177,     0,   178,     0,     0,     0,     0,     0,   198,
     179,     0,     0,     0,   180,     0,   678,   199,     0,     0,
       0,     0,   200,   201,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   181,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   182,     0,   183,
     174,     0,     0,     0,     0,   184,     0,     0,   176,   177,
       0,   178,     0,     0,   185,   186,     0,     0,   179,   187,
     188,     0,   180,   189,   190,   191,   192,   193,     0,   194,
       0,   195,     0,     0,     0,     0,   196,     0,     0,     0,
       0,   197,   181,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   198,     0,     0,   182,     0,   183,     0,   750,
     199,     0,     0,   184,     0,   200,   201,     0,     0,     0,
       0,     0,   185,   186,     0,     0,     0,   187,   188,     0,
       0,   189,   190,   191,   192,   193,     0,   194,     0,   195,
       0,     0,     0,   174,   196,     0,     0,     0,     0,   197,
       0,   176,   177,     0,   178,     0,     0,     0,     0,     0,
     198,   179,     0,     0,     0,   180,     0,     0,   199,     0,
       0,     0,     0,   200,   201,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   181,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   182,     0,
     183,     0,     0,     0,     0,     0,   184,     0,     0,   176,
     177,     0,   178,     0,     0,   185,   186,     0,     0,   179,
     187,   188,     0,   180,   189,   190,   191,   192,   193,     0,
     194,     0,   195,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   197,   181,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   198,     0,     0,   182,     0,   183,     0,
       0,   199,     0,     0,   184,     0,   200,   201,   177,     0,
     178,     0,     0,   185,   186,     0,     0,   179,   187,   188,
       0,   180,   189,   190,   191,   192,   193,     0,   194,     0,
     195,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     197,     0,   375,     0,     0,     0,     0,     0,     0,     0,
       0,   198,     0,     0,   182,     0,   183,     0,     0,   199,
       0,     0,     0,     0,   200,   201,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   187,     0,     0,     0,
     189,   190,     0,   192,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   197,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   198,
       0,     0,     0,     0,     0,     0,     0,   199,     0,     0,
       0,     0,   200,   201
};

static const yytype_int16 yycheck[] =
{
       2,    66,    56,    57,   121,   122,   123,   124,   224,    11,
      12,   105,   179,    15,   548,   354,    18,   252,    20,    69,
     464,   130,   506,   458,   304,   488,    67,    89,   469,    30,
     463,     7,    34,    29,    12,    42,   603,    12,    54,    30,
     105,   509,    42,    40,    51,    47,    50,     3,     3,    41,
     617,    69,    42,    12,    90,    60,    75,    59,    94,    27,
     536,    11,   105,    11,    80,    67,    34,    75,    41,    45,
     164,     9,    42,   109,    79,    39,    14,    77,    97,   136,
      72,    39,    55,   148,   134,   520,   521,   571,   587,    97,
     589,    69,    94,    42,    69,    63,    93,   530,    30,    72,
      97,    30,    58,    58,    30,    11,   103,    77,    46,    24,
     341,   154,    90,     6,    28,    71,    71,   135,    32,   121,
     122,   123,   124,    16,   165,   558,   693,   143,    77,   130,
     574,    90,   113,   135,    30,   137,   100,   600,   145,   233,
      90,   480,    90,   139,   112,   145,   630,   148,   152,   151,
     613,   490,   154,   155,   156,   145,   597,   135,   134,   726,
     135,   328,   153,   165,   632,   609,   446,   122,   124,   124,
     232,   173,   130,   122,   650,   145,   144,   127,    19,   127,
      21,    96,    30,   414,    90,    78,   685,    28,    84,    85,
     148,    32,   408,    27,   361,   127,   145,   155,   127,   366,
      34,   127,   204,   636,   529,   640,   120,   301,   123,   211,
     212,   213,   214,   322,   264,    30,    64,   132,    30,   133,
     134,   639,   263,   641,   226,   227,   228,   229,   230,    63,
      30,   127,    30,   342,    74,    69,    69,    30,   332,    30,
      30,    75,   522,    30,    30,   140,    30,    30,   302,   783,
      91,    85,    46,    94,   256,   257,   258,    90,    38,   722,
     704,   263,    40,   357,   129,   432,   750,   606,   109,   753,
     595,    12,    90,    13,    47,    22,    28,   130,   112,   120,
      32,   335,   336,   337,   338,   339,     0,   128,   523,    36,
     615,    30,   133,   134,   388,   148,    90,   137,   782,    74,
     131,   135,   135,    43,   144,   723,    47,    27,    28,    59,
     144,   151,   127,    60,    34,   127,     0,    90,   368,    39,
      40,   323,   100,    18,     8,     9,    30,   127,   330,   127,
      14,   498,    79,    17,   127,   389,   127,   127,   340,   341,
     127,   127,    94,   127,   127,    84,    85,    31,   350,   351,
      77,    35,    90,   100,   356,    98,   106,   109,    47,   575,
      44,   136,   701,     7,    48,    70,   141,    51,   120,   371,
     372,   710,   466,    57,   468,   150,    52,    97,    73,   108,
     100,   133,   134,    69,   386,   309,   130,   311,   135,    75,
     392,   500,   112,    88,    69,   122,   461,   117,    93,    85,
      75,    45,   145,   130,   148,   100,    93,   115,   139,    28,
      85,   155,   414,    32,    90,   131,   103,    15,   126,   421,
      60,   148,   424,    22,   144,    90,   110,   429,   155,    42,
     116,    74,    30,   136,   118,   119,    49,    36,   547,    79,
     124,   116,    51,    52,    53,   129,   130,   131,   542,   135,
     789,   453,    17,    66,    94,   549,   739,   740,   460,     7,
     135,    60,    69,   147,   148,   467,    90,   469,    75,   109,
      69,   155,   154,   155,   476,   477,    75,    64,    85,   104,
      79,    81,   484,    96,   601,    98,   488,   211,   212,   491,
     136,    47,   105,   106,   107,   141,   109,   499,   121,     5,
      69,   100,    90,   505,   150,    90,    75,   141,   142,   143,
     144,    47,   621,   622,   623,   517,    85,   565,   566,   567,
      90,    90,    47,    47,    56,    12,   528,   134,   135,    70,
      29,    69,   152,   127,    30,   148,   135,    75,    69,   153,
      28,    90,    97,   545,    75,    64,   548,    85,   657,   658,
     163,   131,    90,    84,    85,   557,    90,    69,    30,   127,
     127,   174,   175,    75,   177,   178,   135,   180,   181,   182,
     183,   787,   185,    85,   187,    28,   189,   190,    90,   192,
     193,    47,   127,   196,   197,   198,   199,   200,   201,   202,
     203,    47,   127,    66,    69,   597,    12,   135,   600,   601,
      75,   214,    56,    39,   135,   218,    42,    19,   131,    21,
      85,   613,    69,   127,   679,    90,    28,    81,    75,    23,
      32,   132,   139,   135,    60,     8,     9,    84,    85,    75,
      51,    14,   127,    69,    17,   127,    30,   127,    90,    75,
      68,   643,    23,    79,   646,   127,   259,    90,    31,    85,
      42,    47,    35,    65,   267,    67,   103,    49,    94,    12,
     135,    44,   146,   105,   103,    48,   127,   103,    51,   148,
      90,    69,   127,   109,    57,    87,   106,    75,   135,    91,
      92,    12,    94,   127,   101,    23,    75,    85,   301,    23,
      89,   304,    90,    52,   132,   697,    33,   109,    76,   135,
      90,   103,    86,    69,    96,   127,    98,    84,   120,    75,
      30,   713,   325,   105,   106,   107,   128,   109,   153,    85,
     722,   133,   134,   725,   127,   101,    90,   110,   103,    33,
     103,    12,   127,   107,    30,   118,   119,   135,    30,   127,
      52,   124,    52,    98,    23,   146,   129,   130,   131,    90,
     363,   364,    30,    86,    90,    90,    86,    62,    15,   156,
     373,    90,   375,    90,   147,   148,   127,    90,    52,   135,
     152,   163,   155,    49,   125,   127,    46,   125,     4,   781,
      10,   783,   174,   175,   127,   177,   178,   111,   180,   181,
     182,   183,   117,   185,    61,   187,    40,   189,   190,    22,
     192,   193,    24,   129,   196,   197,   198,   199,   200,   201,
     202,   203,   366,   132,   791,   632,   752,   570,   696,   793,
     629,   434,   214,    83,   388,   205,   218,   233,   607,   482,
     401,   725,   172,   446,   412,     7,   158,   237,   417,   643,
     212,   234,   742,   642,    -1,     8,     9,   780,   461,   462,
     451,    14,    -1,    -1,    17,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    20,    -1,    22,   259,    31,    25,
      26,    -1,    35,    -1,    -1,   267,    -1,    -1,    -1,    -1,
      36,    44,    -1,    -1,    -1,    48,    -1,    -1,    51,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   512,
      56,    -1,    -1,    -1,    60,    -1,    -1,    -1,    -1,   522,
      -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    75,
      -1,    -1,    -1,    79,    -1,    -1,    -1,    -1,    -1,    85,
      -1,    -1,    -1,   325,    90,    -1,    -1,    -1,    94,   552,
     553,    97,    -1,   556,   100,    -1,    -1,   110,   561,    -1,
      -1,   564,    -1,   109,   567,   118,   119,    -1,   114,    -1,
      -1,   124,    -1,    -1,    -1,    -1,   129,   130,    -1,   582,
      -1,   363,   364,    -1,    -1,    -1,    -1,    -1,   134,   135,
      -1,   373,    -1,   375,   147,   148,    -1,    -1,    -1,    -1,
     603,    -1,   155,   149,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   617,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      20,    -1,    22,    -1,   637,    25,    26,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    36,    -1,    -1,    -1,
      -1,    -1,   434,    -1,    -1,    -1,   659,    -1,   661,   662,
     663,    -1,   665,    -1,    -1,    -1,    56,    -1,    -1,    -1,
      60,    -1,    -1,    -1,    -1,    -1,   679,    -1,    -1,    69,
     462,    -1,    -1,    -1,    -1,    75,   689,    -1,    -1,    79,
     693,    -1,    -1,    -1,    -1,    85,    -1,    -1,    -1,    -1,
      90,    -1,    -1,    -1,    94,    -1,    -1,    97,    -1,    -1,
     100,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   721,   109,
      -1,    -1,    -1,   726,   114,    -1,    -1,    -1,   731,    -1,
     512,    -1,   735,    -1,    18,    -1,    20,    -1,    22,   742,
     130,    25,    26,    -1,    -1,   135,    -1,    -1,    -1,    -1,
      -1,    -1,    36,    37,    -1,    -1,    -1,    -1,   148,   149,
      -1,    -1,    -1,    -1,    -1,   155,    -1,    -1,    -1,    -1,
     552,   553,    56,    -1,   556,    -1,    60,   780,    -1,   561,
      -1,    -1,   564,    -1,    -1,    69,    -1,    -1,    -1,    -1,
      -1,    75,    -1,    -1,    -1,    79,    -1,    -1,    -1,    -1,
     582,    85,    -1,    -1,    -1,    -1,    90,    -1,    -1,    -1,
      94,    -1,    -1,    97,    -1,    -1,   100,    -1,    -1,    -1,
      -1,   603,    -1,    -1,    -1,   109,    -1,    -1,    -1,    -1,
     114,    -1,    -1,    -1,    -1,   617,    20,    -1,    22,    -1,
      -1,    25,    26,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   135,    36,    37,    -1,   637,    -1,    -1,   142,    -1,
      -1,    -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,    -1,
      -1,    -1,    56,    -1,    -1,    -1,    60,   659,    -1,   661,
     662,   663,    20,   665,    22,    69,    -1,    25,    26,    -1,
      -1,    75,    -1,    -1,    -1,    79,    -1,    -1,    36,    37,
      -1,    85,    -1,    -1,    -1,    -1,    90,   689,    -1,    -1,
      94,   693,    -1,    97,    -1,    -1,   100,    -1,    56,    -1,
      -1,    -1,    60,    -1,    -1,   109,    -1,    -1,    -1,    -1,
     114,    69,    -1,    -1,    -1,    -1,    -1,    75,    -1,   721,
      -1,    79,    -1,    -1,   726,    -1,    -1,    85,    -1,   731,
      -1,   135,    90,   735,    -1,    -1,    94,    -1,   142,    97,
     742,    -1,   100,    -1,    -1,   149,    -1,    20,    -1,    22,
      -1,   109,    25,    26,    -1,    -1,   114,    -1,    -1,    -1,
      -1,    -1,    -1,    36,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    10,    -1,    -1,    -1,    -1,    -1,   135,   780,    18,
      19,    -1,    21,    56,   142,    -1,    -1,    60,    -1,    28,
      -1,   149,    -1,    32,    -1,    -1,    69,    -1,    -1,    -1,
      -1,    -1,    75,    -1,    -1,    -1,    79,    -1,    -1,    -1,
      -1,    -1,    85,    52,    -1,    -1,    -1,    90,    -1,    -1,
      -1,    94,    -1,    -1,    97,    -1,    65,   100,    67,    10,
      -1,    -1,    -1,    -1,    73,    -1,   109,    18,    19,    -1,
      21,   114,    -1,    82,    83,    -1,    -1,    28,    87,    88,
      -1,    32,    91,    92,    93,    94,    95,    -1,    97,    -1,
      99,    -1,   135,    -1,    -1,   104,    -1,    -1,    -1,    -1,
     109,    52,    -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,
      -1,   120,    -1,    -1,    65,    -1,    67,    -1,    -1,   128,
      -1,    -1,    73,    -1,   133,   134,    -1,    -1,    -1,   138,
      -1,    82,    83,    -1,    -1,    -1,    87,    88,    -1,    -1,
      91,    92,    93,    94,    95,    -1,    97,    -1,    99,    -1,
      -1,    -1,    10,   104,    -1,    13,    -1,    -1,   109,    -1,
      18,    19,    -1,    21,    -1,    -1,    -1,    -1,    -1,   120,
      28,    -1,    -1,    -1,    32,    -1,    -1,   128,    -1,    -1,
      -1,    -1,   133,   134,    -1,    43,    -1,   138,    -1,    -1,
      -1,    -1,    -1,    -1,    52,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    65,    -1,    67,
      -1,    -1,    -1,    -1,    -1,    73,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    82,    83,    -1,    -1,    -1,    87,
      88,    -1,    -1,    91,    92,    93,    94,    95,    -1,    97,
      -1,    99,    -1,    -1,    -1,    10,   104,    -1,    -1,    -1,
      -1,   109,    -1,    18,    19,    -1,    21,    -1,    -1,    -1,
      -1,    -1,   120,    28,    -1,    30,    -1,    32,    -1,    -1,
     128,    -1,    -1,    -1,    -1,   133,   134,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    52,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      65,    -1,    67,    10,    -1,    -1,    -1,    -1,    73,    -1,
      -1,    18,    19,    -1,    21,    -1,    -1,    82,    83,    -1,
      -1,    28,    87,    88,    -1,    32,    91,    92,    93,    94,
      95,    -1,    97,    -1,    99,    -1,    -1,   102,    -1,   104,
      -1,    -1,    -1,    -1,   109,    52,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    61,   120,    -1,    -1,    65,    -1,
      67,    -1,    -1,   128,    -1,    -1,    73,    -1,   133,   134,
      -1,    -1,    -1,    -1,    -1,    82,    83,    -1,    -1,    -1,
      87,    88,    -1,    -1,    91,    92,    93,    94,    95,    -1,
      97,    -1,    99,    -1,    -1,    -1,    -1,   104,    10,    -1,
      12,    -1,   109,    -1,   111,    -1,    18,    19,    -1,    21,
      -1,    -1,    -1,   120,    -1,    -1,    28,    -1,    -1,    -1,
      32,   128,    -1,    -1,    -1,    -1,   133,   134,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      52,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    65,    -1,    67,    10,    -1,    -1,    -1,
      -1,    73,    -1,    -1,    18,    19,    -1,    21,    -1,    -1,
      82,    83,    -1,    -1,    28,    87,    88,    -1,    32,    91,
      92,    93,    94,    95,    -1,    97,    -1,    99,    -1,    -1,
      -1,    -1,   104,    -1,    -1,    -1,    -1,   109,    52,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   120,    -1,
      -1,    65,    -1,    67,    -1,    -1,   128,    -1,    -1,    73,
      -1,   133,   134,    -1,    -1,    -1,    -1,    -1,    82,    83,
      -1,    -1,    -1,    87,    88,    -1,    -1,    91,    92,    93,
      94,    95,    -1,    97,    -1,    99,    -1,    -1,    -1,    10,
     104,    12,    -1,    -1,    -1,   109,    -1,    18,    19,    -1,
      21,    -1,    -1,    -1,    -1,    -1,   120,    28,    -1,    -1,
      -1,    32,    -1,   127,   128,    -1,    -1,    -1,    -1,   133,
     134,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    52,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    65,    -1,    67,    10,    -1,    -1,
      -1,    -1,    73,    -1,    -1,    18,    19,    -1,    21,    -1,
      -1,    82,    83,    -1,    -1,    28,    87,    88,    -1,    32,
      91,    92,    93,    94,    95,    -1,    97,    -1,    99,    -1,
      -1,    -1,    -1,   104,    -1,    -1,    -1,    -1,   109,    52,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   120,
      -1,    -1,    65,    -1,    67,    -1,    -1,   128,    -1,    -1,
      73,    -1,   133,   134,    -1,    -1,    -1,    -1,    -1,    82,
      83,    -1,    -1,    86,    87,    88,    -1,    -1,    91,    92,
      93,    94,    95,    -1,    97,    -1,    99,    -1,    -1,    -1,
      10,   104,    -1,    -1,    -1,    -1,   109,    -1,    18,    19,
      -1,    21,    -1,    -1,    -1,    -1,    -1,   120,    28,    -1,
      -1,    -1,    32,    -1,    -1,   128,    -1,    -1,    -1,    -1,
     133,   134,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    52,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    65,    -1,    67,    10,    -1,
      -1,    -1,    -1,    73,    -1,    -1,    18,    19,    -1,    21,
      -1,    -1,    82,    83,    -1,    -1,    28,    87,    88,    -1,
      32,    91,    92,    93,    94,    95,    -1,    97,    -1,    99,
      -1,    -1,    -1,    -1,   104,    -1,    -1,    -1,    -1,   109,
      52,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     120,    -1,    -1,    65,    -1,    67,    -1,   127,   128,    -1,
      -1,    73,    -1,   133,   134,    -1,    -1,    -1,    -1,    -1,
      82,    83,    -1,    -1,    -1,    87,    88,    -1,    -1,    91,
      92,    93,    94,    95,    -1,    97,    -1,    99,    -1,    -1,
      -1,    10,   104,    -1,    -1,    -1,    -1,   109,    -1,    18,
      19,    -1,    21,    -1,    -1,    -1,    -1,    -1,   120,    28,
      -1,    -1,    -1,    32,    -1,   127,   128,    -1,    -1,    -1,
      -1,   133,   134,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    52,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    65,    -1,    67,    10,
      -1,    -1,    -1,    -1,    73,    -1,    -1,    18,    19,    -1,
      21,    -1,    -1,    82,    83,    -1,    -1,    28,    87,    88,
      -1,    32,    91,    92,    93,    94,    95,    -1,    97,    -1,
      99,    -1,    -1,    -1,    -1,   104,    -1,    -1,    -1,    -1,
     109,    52,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   120,    -1,    -1,    65,    -1,    67,    -1,   127,   128,
      -1,    -1,    73,    -1,   133,   134,    -1,    -1,    -1,    -1,
      -1,    82,    83,    -1,    -1,    -1,    87,    88,    -1,    -1,
      91,    92,    93,    94,    95,    -1,    97,    -1,    99,    -1,
      -1,    -1,    10,   104,    -1,    -1,    -1,    -1,   109,    -1,
      18,    19,    -1,    21,    -1,    -1,    -1,    -1,    -1,   120,
      28,    -1,    -1,    -1,    32,    -1,   127,   128,    -1,    -1,
      -1,    -1,   133,   134,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    52,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    65,    -1,    67,
      10,    -1,    -1,    -1,    -1,    73,    -1,    -1,    18,    19,
      -1,    21,    -1,    -1,    82,    83,    -1,    -1,    28,    87,
      88,    -1,    32,    91,    92,    93,    94,    95,    -1,    97,
      -1,    99,    -1,    -1,    -1,    -1,   104,    -1,    -1,    -1,
      -1,   109,    52,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   120,    -1,    -1,    65,    -1,    67,    -1,   127,
     128,    -1,    -1,    73,    -1,   133,   134,    -1,    -1,    -1,
      -1,    -1,    82,    83,    -1,    -1,    -1,    87,    88,    -1,
      -1,    91,    92,    93,    94,    95,    -1,    97,    -1,    99,
      -1,    -1,    -1,    10,   104,    -1,    -1,    -1,    -1,   109,
      -1,    18,    19,    -1,    21,    -1,    -1,    -1,    -1,    -1,
     120,    28,    -1,    -1,    -1,    32,    -1,    -1,   128,    -1,
      -1,    -1,    -1,   133,   134,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    52,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    65,    -1,
      67,    -1,    -1,    -1,    -1,    -1,    73,    -1,    -1,    18,
      19,    -1,    21,    -1,    -1,    82,    83,    -1,    -1,    28,
      87,    88,    -1,    32,    91,    92,    93,    94,    95,    -1,
      97,    -1,    99,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   109,    52,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   120,    -1,    -1,    65,    -1,    67,    -1,
      -1,   128,    -1,    -1,    73,    -1,   133,   134,    19,    -1,
      21,    -1,    -1,    82,    83,    -1,    -1,    28,    87,    88,
      -1,    32,    91,    92,    93,    94,    95,    -1,    97,    -1,
      99,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     109,    -1,    53,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   120,    -1,    -1,    65,    -1,    67,    -1,    -1,   128,
      -1,    -1,    -1,    -1,   133,   134,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    87,    -1,    -1,    -1,
      91,    92,    -1,    94,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   109,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   120,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   128,    -1,    -1,
      -1,    -1,   133,   134
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     8,     9,    14,    17,    31,    35,    44,    48,    51,
      57,   110,   118,   119,   124,   129,   130,   131,   147,   148,
     155,   158,   159,   160,   161,   162,   163,   167,   196,   197,
     199,   200,   259,   265,   136,    69,    75,    85,   135,   174,
     279,    38,   256,    41,    55,    72,   165,   140,   164,   256,
      74,   136,   141,   150,   164,   113,   174,   174,   129,   166,
     164,   174,     7,    45,   201,   174,    90,   116,   174,   266,
       0,   160,   162,   131,   137,   144,   151,   169,   238,   245,
      54,    80,   143,   198,    30,    90,    42,    77,   122,   145,
     224,   174,   209,   257,    47,   208,    20,    22,    25,    26,
      36,    56,    60,    79,    85,    90,    94,    97,   100,   109,
     114,   135,   149,   174,   227,   228,   279,   164,   174,   227,
      70,   195,   195,   195,   195,   108,   208,   208,   174,   139,
     202,   203,   227,   236,   266,    90,   239,    30,   197,   131,
     136,   136,   141,   150,    74,    17,     7,   199,    90,   260,
     261,    64,   104,   193,   193,    81,    47,   121,     5,   174,
     227,   227,   234,    90,    90,   155,   196,   227,   236,   227,
     227,   227,    90,    47,    10,    12,    18,    19,    21,    28,
      32,    52,    65,    67,    73,    82,    83,    87,    88,    91,
      92,    93,    94,    95,    97,    99,   104,   109,   120,   128,
     133,   134,   229,   230,   231,    90,    56,   209,   209,   209,
     209,    52,    90,   166,   178,    30,    64,   205,    30,   127,
     197,   174,   240,    12,   174,    70,   168,   168,   168,   168,
     168,    42,   145,   178,   250,   253,   236,    30,   127,    11,
      90,   262,   263,   174,   210,     3,    58,    71,   122,   124,
     194,   254,   210,   210,   174,    29,   139,   258,   258,   152,
     232,   227,   196,   116,   266,   127,   127,    30,    71,   254,
     174,   227,   227,   227,   227,    69,   135,   280,   227,   227,
     227,   227,   227,   227,   227,   227,   227,   227,    18,    73,
      88,    93,   100,   227,   227,   227,   227,   227,   227,   227,
     227,    90,   174,   134,   201,    39,    42,    60,    79,    94,
     103,   109,   174,   242,   243,   244,   281,   242,   244,   174,
     134,   174,   227,   206,   207,   153,   221,   227,    28,   241,
      30,   127,    90,   239,    97,   174,   174,   174,   174,   174,
      64,   193,   196,   224,    51,   253,   131,   127,   261,   264,
      12,    47,    75,    97,   213,   213,    90,   225,   174,   174,
     173,   174,   227,    50,   152,   233,    12,   127,   266,   227,
     127,    30,    47,    86,   255,    53,   196,   235,   236,   208,
     127,   235,   281,   281,   127,   127,    47,   178,    90,   174,
      30,    84,    85,   211,   227,    66,   218,   280,    13,    43,
     217,   174,   196,    12,    56,   208,   208,   208,   208,   208,
     174,   251,   251,   178,    81,   131,    11,    90,   127,   174,
     174,    23,    75,   221,   132,   174,   226,    39,   196,   139,
     179,   175,   176,   280,   138,   227,   227,    51,   175,   127,
     174,   174,   227,   227,   127,   127,    90,   237,    59,   277,
     278,   127,   134,    12,   204,   280,   196,   207,   208,    84,
     174,    23,    68,   219,   241,   127,    90,   146,    12,    90,
     170,     6,    16,    78,   246,   239,   103,    47,    75,    97,
     252,   252,   251,   264,    12,   174,   105,   215,    90,   174,
     222,    30,   127,   148,   103,   223,   174,    27,    28,    34,
      39,    40,    97,   100,   112,   117,   144,   180,   185,    90,
     280,   227,   138,   127,   127,   235,    90,   106,   277,   174,
     127,   127,    90,   204,    84,   174,   236,   227,   154,   215,
     276,   217,   196,   174,   196,   172,   173,   101,    42,    77,
     145,   247,    12,   174,   174,    23,    75,   221,   132,   225,
     127,   174,    23,    89,   220,   226,    52,    30,   221,   174,
      33,    90,   280,   174,    90,    94,   109,   178,    76,   186,
      40,   100,   103,   192,    86,   174,   192,   177,   243,   244,
     227,   127,   153,    90,   174,   269,   204,   204,   235,   213,
      84,   174,   267,   268,   220,   215,   127,    30,   187,   179,
     101,   103,   196,    90,   174,   178,   222,   196,   216,   227,
     227,   127,   227,    90,   174,   215,    46,    90,   227,   227,
     135,   228,   228,   228,   279,    41,    72,   186,   192,    33,
     217,   239,    30,   127,   227,   107,   270,   103,   212,   212,
     127,   212,    12,    30,   220,    27,    34,    63,   112,   144,
     173,   188,   190,   127,   226,   209,   216,   221,   223,    30,
     217,    30,   102,    52,   226,    52,   220,    98,   216,   127,
     127,   178,   178,   178,   194,   192,   182,   177,   127,    23,
     215,   227,   146,   214,   214,   204,   214,   269,   268,    90,
     174,    86,    86,    90,   179,    30,   189,   156,   171,    62,
     248,   127,   178,   178,   227,   227,   227,   227,   127,   227,
     127,    15,   181,    93,   103,   183,   127,   236,   115,   126,
     271,   272,    90,   212,   227,    90,    90,   216,   190,   174,
      49,   152,   249,   221,   217,    52,   221,   174,    42,    77,
     145,   127,    18,    37,   142,   227,   273,   275,   226,   214,
     127,   240,   216,   127,   125,   227,   227,    46,    24,    96,
     123,   132,   184,   184,   184,   273,   125,   111,    61,   111,
     127,   192,   127,   181,   192,    98,   145,     4,    39,   100,
      10,   117,   127,   132,   142,   274,   275,   174,   192,   222,
      61,   239,   221,   182,    97,   185,   191
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   157,   158,   159,   159,   160,   160,   160,   161,   161,
     162,   163,   163,   163,   163,   163,   163,   163,   163,   163,
     163,   163,   163,   163,   163,   163,   163,   163,   163,   163,
     163,   163,   163,   163,   163,   163,   163,   163,   163,   163,
     163,   163,   163,   163,   163,   163,   163,   163,   163,   164,
     164,   164,   165,   165,   165,   165,   166,   166,   167,   168,
     168,   169,   169,   170,   170,   171,   171,   172,   172,   173,
     174,   174,   174,   175,   175,   175,   175,   176,   176,   177,
     177,   178,   179,   179,   180,   180,   180,   180,   180,   180,
     180,   180,   180,   180,   180,   180,   180,   180,   181,   181,
     182,   182,   183,   183,   183,   183,   184,   184,   184,   184,
     184,   185,   185,   186,   186,   186,   187,   187,   188,   188,
     189,   189,   190,   190,   190,   190,   190,   191,   191,   192,
     192,   193,   193,   194,   194,   194,   195,   195,   196,   196,
     196,   197,   197,   198,   198,   198,   198,   199,   199,   199,
     200,   200,   201,   201,   201,   202,   202,   203,   203,   203,
     204,   204,   204,   205,   205,   206,   206,   207,   207,   207,
     207,   208,   208,   209,   209,   210,   210,   210,   210,   211,
     211,   211,   211,   211,   212,   212,   213,   213,   213,   214,
     214,   215,   215,   216,   216,   217,   217,   217,   218,   218,
     219,   219,   220,   220,   220,   220,   221,   221,   222,   222,
     222,   222,   223,   223,   223,   223,   224,   224,   225,   225,
     226,   226,   227,   227,   227,   227,   227,   227,   227,   227,
     227,   227,   227,   227,   227,   227,   227,   227,   227,   227,
     227,   227,   227,   227,   227,   227,   227,   227,   227,   227,
     227,   227,   227,   227,   227,   227,   227,   227,   227,   227,
     227,   227,   227,   227,   227,   227,   227,   227,   227,   227,
     227,   227,   227,   228,   228,   228,   228,   228,   228,   229,
     229,   229,   229,   230,   230,   231,   231,   232,   232,   233,
     233,   234,   234,   235,   235,   236,   236,   237,   237,   238,
     238,   239,   239,   240,   240,   241,   241,   242,   242,   242,
     242,   242,   243,   243,   244,   245,   246,   246,   246,   246,
     247,   247,   247,   247,   248,   248,   249,   249,   250,   250,
     251,   251,   252,   252,   252,   253,   253,   253,   253,   254,
     254,   254,   255,   255,   256,   256,   257,   258,   258,   259,
     260,   260,   261,   261,   262,   262,   263,   264,   264,   264,
     265,   265,   265,   266,   266,   267,   267,   268,   269,   270,
     270,   271,   271,   271,   272,   272,   273,   273,   274,   274,
     275,   275,   275,   276,   277,   277,   278,   278,   279,   279,
     280,   280,   281,   281
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     2,     1,     1,     2,     3,     1,     3,
       1,     3,     2,     2,     2,     2,     3,     5,     7,     4,
       9,     4,     1,     8,    10,     7,     7,    12,     4,     1,
       2,     3,     5,     6,     5,     6,     5,     4,     6,     3,
       1,     3,     1,     3,     6,     7,     8,     1,     4,     0,
       1,     2,     0,     1,     1,     1,     1,     0,     1,     0,
       3,     1,     0,     5,     2,     0,     2,     4,     2,     2,
       1,     1,     1,     0,     1,     4,     6,     1,     2,     1,
       1,     0,     2,     0,     2,     4,     4,     4,     4,     3,
       2,     3,     5,     2,     4,     4,     1,     2,     0,     1,
       0,     2,     2,     3,     3,     3,     2,     2,     1,     1,
       2,     3,     2,     0,     2,     2,     0,     2,     3,     1,
       1,     0,     2,     7,     5,     5,    10,     0,     1,     0,
       3,     0,     2,     1,     1,     1,     2,     0,     3,     4,
       1,     1,     3,     1,     2,     1,     1,     9,    10,     1,
       4,     5,     1,     1,     0,     2,     0,     5,     3,     5,
       2,     1,     0,     0,     2,     2,     0,     7,     9,     7,
       7,     0,     2,     1,     3,     1,     3,     5,     3,     1,
       1,     2,     3,     4,     2,     0,     0,     3,     2,     4,
       0,     0,     3,     4,     2,     1,     1,     0,     0,     3,
       0,     2,     0,     2,     4,     4,     0,     2,     5,     7,
       3,     5,     0,    11,     8,     4,     2,     1,     0,     3,
       3,     1,     1,     3,     1,     1,     3,     5,     1,     3,
       6,     5,     4,     6,     5,     5,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     5,     2,     2,     3,     3,
       2,     2,     2,     2,     3,     5,     3,     5,     5,     4,
       5,     4,     6,     1,     1,     1,     1,     1,     1,     1,
       1,     2,     2,     1,     2,     1,     2,     5,     4,     2,
       0,     1,     0,     1,     0,     3,     1,     0,     3,     1,
       0,     0,     3,     5,     3,     0,     2,     1,     1,     1,
       1,     1,     2,     1,     2,    11,     1,     1,     2,     0,
       1,     1,     1,     3,     0,     3,     0,     2,     3,     2,
       1,     3,     0,     3,     2,     8,     8,     6,     3,     1,
       1,     1,     0,     2,     1,     0,     1,     0,     1,     8,
       1,     3,     0,     2,     1,     3,     1,     0,     4,     2,
       0,     2,     3,     6,     8,     1,     3,     3,     5,     3,
       0,     0,     2,     5,     1,     1,     1,     2,     1,     2,
       2,     2,     2,     2,     3,     3,     0,     5,     1,     1,
       1,     1,     1,     1
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                  \
do                                                              \
  if (yychar == YYEMPTY)                                        \
    {                                                           \
      yychar = (Token);                                         \
      yylval = (Value);                                         \
      YYPOPSTACK (yylen);                                       \
      yystate = *yyssp;                                         \
      goto yybackup;                                            \
    }                                                           \
  else                                                          \
    {                                                           \
      yyerror (parse_tree, scanner, YY_("syntax error: cannot back up")); \
      YYERROR;                                                  \
    }                                                           \
while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256



/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)

/* This macro is provided for backward compatibility. */
#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value, parse_tree, scanner); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, parse_node **parse_tree, yyscan_t scanner)
{
  FILE *yyoutput = yyo;
  YYUSE (yyoutput);
  YYUSE (parse_tree);
  YYUSE (scanner);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyo, yytoknum[yytype], *yyvaluep);
# endif
  YYUSE (yytype);
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, parse_node **parse_tree, yyscan_t scanner)
{
  YYFPRINTF (yyo, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  yy_symbol_value_print (yyo, yytype, yyvaluep, parse_tree, scanner);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yytype_int16 *yyssp, YYSTYPE *yyvsp, int yyrule, parse_node **parse_tree, yyscan_t scanner)
{
  unsigned long yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[yyssp[yyi + 1 - yynrhs]],
                       &(yyvsp[(yyi + 1) - (yynrhs)])
                                              , parse_tree, scanner);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, parse_tree, scanner); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
static YYSIZE_T
yystrlen (const char *yystr)
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            /* Fall through.  */
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return (YYSIZE_T) (yystpcpy (yyres, yystr) - yyres);
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYSIZE_T yysize1 = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (! (yysize <= yysize1
                         && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                    return 2;
                  yysize = yysize1;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    YYSIZE_T yysize1 = yysize + yystrlen (yyformat);
    if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
      return 2;
    yysize = yysize1;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, parse_node **parse_tree, yyscan_t scanner)
{
  YYUSE (yyvaluep);
  YYUSE (parse_tree);
  YYUSE (scanner);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/*----------.
| yyparse.  |
`----------*/

int
yyparse (parse_node **parse_tree, yyscan_t scanner)
{
/* The lookahead symbol.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = (yytype_int16) yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = (YYSIZE_T) (yyssp - yyss + 1);

#ifdef yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        YYSTYPE *yyvs1 = yyvs;
        yytype_int16 *yyss1 = yyss;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * sizeof (*yyssp),
                    &yyvs1, yysize * sizeof (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yytype_int16 *yyss1 = yyss;
        union yyalloc *yyptr =
          (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
                  (unsigned long) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex (&yylval, scanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 10:
#line 245 "parser.y" /* yacc.c:1660  */
    { *parse_tree = (yyvsp[0].node); }
#line 2377 "parser.c" /* yacc.c:1660  */
    break;

  case 11:
#line 249 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "BEGIN not yet supported"); }
#line 2383 "parser.c" /* yacc.c:1660  */
    break;

  case 12:
#line 250 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "COMMIT not yet supported"); }
#line 2389 "parser.c" /* yacc.c:1660  */
    break;

  case 13:
#line 251 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "END not yet supported"); }
#line 2395 "parser.c" /* yacc.c:1660  */
    break;

  case 14:
#line 252 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ROLLBACK not yet supported"); }
#line 2401 "parser.c" /* yacc.c:1660  */
    break;

  case 15:
#line 253 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "SAVEPOINT not yet supported"); }
#line 2407 "parser.c" /* yacc.c:1660  */
    break;

  case 16:
#line 254 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "RELEASE not yet supported"); }
#line 2413 "parser.c" /* yacc.c:1660  */
    break;

  case 17:
#line 255 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ROLLBACK not yet supported"); }
#line 2419 "parser.c" /* yacc.c:1660  */
    break;

  case 18:
#line 256 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "CREATE TABLE not yet supported"); }
#line 2425 "parser.c" /* yacc.c:1660  */
    break;

  case 19:
#line 257 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "DROP TABLE not yet supported"); }
#line 2431 "parser.c" /* yacc.c:1660  */
    break;

  case 20:
#line 258 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "CREATE VIEW not yet supported"); }
#line 2437 "parser.c" /* yacc.c:1660  */
    break;

  case 21:
#line 259 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "DROP VIEW not yet supported"); }
#line 2443 "parser.c" /* yacc.c:1660  */
    break;

  case 22:
#line 260 "parser.y" /* yacc.c:1660  */
    { (yyval.node) = (yyvsp[0].node); }
#line 2449 "parser.c" /* yacc.c:1660  */
    break;

  case 23:
#line 261 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "DELETE not yet supported"); }
#line 2455 "parser.c" /* yacc.c:1660  */
    break;

  case 24:
#line 262 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "UPDATE not yet supported"); }
#line 2461 "parser.c" /* yacc.c:1660  */
    break;

  case 25:
#line 263 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "INSERT not yet supported"); }
#line 2467 "parser.c" /* yacc.c:1660  */
    break;

  case 26:
#line 264 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "INSERT not yet supported"); }
#line 2473 "parser.c" /* yacc.c:1660  */
    break;

  case 27:
#line 265 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "CREATE INDEX not yet supported"); }
#line 2479 "parser.c" /* yacc.c:1660  */
    break;

  case 28:
#line 266 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "DROP INDEX not yet supported"); }
#line 2485 "parser.c" /* yacc.c:1660  */
    break;

  case 29:
#line 267 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "VACUUM not yet supported"); }
#line 2491 "parser.c" /* yacc.c:1660  */
    break;

  case 30:
#line 268 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "VACUUM not yet supported"); }
#line 2497 "parser.c" /* yacc.c:1660  */
    break;

  case 31:
#line 269 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
#line 2503 "parser.c" /* yacc.c:1660  */
    break;

  case 32:
#line 270 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
#line 2509 "parser.c" /* yacc.c:1660  */
    break;

  case 33:
#line 271 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
#line 2515 "parser.c" /* yacc.c:1660  */
    break;

  case 34:
#line 272 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
#line 2521 "parser.c" /* yacc.c:1660  */
    break;

  case 35:
#line 273 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
#line 2527 "parser.c" /* yacc.c:1660  */
    break;

  case 36:
#line 274 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "CREATE BEGIN END not yet supported"); }
#line 2533 "parser.c" /* yacc.c:1660  */
    break;

  case 37:
#line 275 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "DROP TRIGGER not yet supported"); }
#line 2539 "parser.c" /* yacc.c:1660  */
    break;

  case 38:
#line 276 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ATTACH not yet supported"); }
#line 2545 "parser.c" /* yacc.c:1660  */
    break;

  case 39:
#line 277 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "DETACH not yet supported"); }
#line 2551 "parser.c" /* yacc.c:1660  */
    break;

  case 40:
#line 278 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "REINDEX not yet supported"); }
#line 2557 "parser.c" /* yacc.c:1660  */
    break;

  case 41:
#line 279 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "REINDEX not yet supported"); }
#line 2563 "parser.c" /* yacc.c:1660  */
    break;

  case 42:
#line 280 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ANALYZE not yet supported"); }
#line 2569 "parser.c" /* yacc.c:1660  */
    break;

  case 43:
#line 281 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ANALYZE not yet supported"); }
#line 2575 "parser.c" /* yacc.c:1660  */
    break;

  case 44:
#line 282 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ALTER TABLE not yet supported"); }
#line 2581 "parser.c" /* yacc.c:1660  */
    break;

  case 45:
#line 283 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ALTER TABLE not yet supported"); }
#line 2587 "parser.c" /* yacc.c:1660  */
    break;

  case 46:
#line 284 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ALTER TABLE not yet supported"); }
#line 2593 "parser.c" /* yacc.c:1660  */
    break;

  case 47:
#line 285 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "VIRTUAL TABLE not yet supported"); }
#line 2599 "parser.c" /* yacc.c:1660  */
    break;

  case 48:
#line 286 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "VIRTUAL TABLE not yet supported"); }
#line 2605 "parser.c" /* yacc.c:1660  */
    break;

  case 70:
#line 341 "parser.y" /* yacc.c:1660  */
    { (yyval.strval) = (yyvsp[0].strval); }
#line 2611 "parser.c" /* yacc.c:1660  */
    break;

  case 71:
#line 342 "parser.y" /* yacc.c:1660  */
    { (yyval.strval) = (yyvsp[0].strval); }
#line 2617 "parser.c" /* yacc.c:1660  */
    break;

  case 72:
#line 343 "parser.y" /* yacc.c:1660  */
    { (yyval.strval) = (yyvsp[0].strval); }
#line 2623 "parser.c" /* yacc.c:1660  */
    break;

  case 138:
#line 475 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "WITH SELECT not yet supported"); }
#line 2629 "parser.c" /* yacc.c:1660  */
    break;

  case 139:
#line 476 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "WITH RECURSIVE SELECT not yet supported"); }
#line 2635 "parser.c" /* yacc.c:1660  */
    break;

  case 140:
#line 477 "parser.y" /* yacc.c:1660  */
    { (yyval.node) = (yyvsp[0].node); }
#line 2641 "parser.c" /* yacc.c:1660  */
    break;

  case 141:
#line 481 "parser.y" /* yacc.c:1660  */
    { (yyval.node) = (yyvsp[0].node); }
#line 2647 "parser.c" /* yacc.c:1660  */
    break;

  case 142:
#line 482 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "multiselect not yet supported"); }
#line 2653 "parser.c" /* yacc.c:1660  */
    break;

  case 147:
#line 493 "parser.y" /* yacc.c:1660  */
    {
    (yyval.node) = alloc_node("ParseStatementSetOperation");

    parse_node *select_query = alloc_node("ParseSetOperation");
    add_child((yyval.node), "set_operation_query", select_query);
    add_attribute(select_query, "set_operation_type", "Select");

    dynamic_array *operands = alloc_array();
    add_child_list(select_query, "operands", operands);

    parse_node *select_child = alloc_node("ParseSelect");
    add_last(operands, select_child);

    add_child_list(select_child, "select", (yyvsp[-6].list));
    add_child_list(select_child, "from", (yyvsp[-5].list));

    if ((yyvsp[-3].list)) {
      add_child_list(select_child, "group_by", (yyvsp[-3].list));
    }
  }
#line 2678 "parser.c" /* yacc.c:1660  */
    break;

  case 148:
#line 513 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "window queries not yet supported"); }
#line 2684 "parser.c" /* yacc.c:1660  */
    break;

  case 149:
#line 514 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "VALUES not yet supported"); }
#line 2690 "parser.c" /* yacc.c:1660  */
    break;

  case 155:
#line 529 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = (yyvsp[-1].list);
  }
#line 2698 "parser.c" /* yacc.c:1660  */
    break;

  case 156:
#line 532 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = alloc_array();
  }
#line 2706 "parser.c" /* yacc.c:1660  */
    break;

  case 157:
#line 538 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = (yyvsp[-4].list);
    add_last((yyval.list), (yyvsp[-2].node));
  }
#line 2715 "parser.c" /* yacc.c:1660  */
    break;

  case 158:
#line 542 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "SELECT *  not yet supported"); }
#line 2721 "parser.c" /* yacc.c:1660  */
    break;

  case 159:
#line 543 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "SELECT .* not yet supported"); }
#line 2727 "parser.c" /* yacc.c:1660  */
    break;

  case 163:
#line 553 "parser.y" /* yacc.c:1660  */
    { (yyval.list) = 0; }
#line 2733 "parser.c" /* yacc.c:1660  */
    break;

  case 164:
#line 554 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = (yyvsp[0].list);
  }
#line 2741 "parser.c" /* yacc.c:1660  */
    break;

  case 167:
#line 565 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = alloc_array();
    parse_node *table_ref = alloc_node("ParseSimpleTableReference");
    add_attribute(table_ref, "table_name", (yyvsp[-5].strval));
    add_last((yyval.list), table_ref);
  }
#line 2752 "parser.c" /* yacc.c:1660  */
    break;

  case 168:
#line 571 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "parentheses in FROM clause not yet supported"); }
#line 2758 "parser.c" /* yacc.c:1660  */
    break;

  case 169:
#line 572 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "nested select not yet supported"); }
#line 2764 "parser.c" /* yacc.c:1660  */
    break;

  case 170:
#line 573 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "parentheses in FROM clause not yet supported"); }
#line 2770 "parser.c" /* yacc.c:1660  */
    break;

  case 198:
#line 634 "parser.y" /* yacc.c:1660  */
    { (yyval.list) = 0; }
#line 2776 "parser.c" /* yacc.c:1660  */
    break;

  case 199:
#line 635 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = (yyvsp[0].list);
  }
#line 2784 "parser.c" /* yacc.c:1660  */
    break;

  case 222:
#line 687 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "expression terms not yet supported"); }
#line 2790 "parser.c" /* yacc.c:1660  */
    break;

  case 223:
#line 688 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "parentheses in expression not yet supported"); }
#line 2796 "parser.c" /* yacc.c:1660  */
    break;

  case 224:
#line 689 "parser.y" /* yacc.c:1660  */
    {
    (yyval.node) = alloc_node("AttributeReference");
    add_attribute((yyval.node), "attribute_name", (yyvsp[0].strval));
  }
#line 2805 "parser.c" /* yacc.c:1660  */
    break;

  case 225:
#line 693 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "join keyword in expression not yet supported"); }
#line 2811 "parser.c" /* yacc.c:1660  */
    break;

  case 226:
#line 694 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "nm.nm in expression not yet supported"); }
#line 2817 "parser.c" /* yacc.c:1660  */
    break;

  case 227:
#line 695 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "nm.nm.nm in expression not yet supported"); }
#line 2823 "parser.c" /* yacc.c:1660  */
    break;

  case 228:
#line 696 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "VARIABLE not yet supported"); }
#line 2829 "parser.c" /* yacc.c:1660  */
    break;

  case 229:
#line 697 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "COLLATE not yet supported"); }
#line 2835 "parser.c" /* yacc.c:1660  */
    break;

  case 230:
#line 698 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "CAST not yet supported"); }
#line 2841 "parser.c" /* yacc.c:1660  */
    break;

  case 231:
#line 699 "parser.y" /* yacc.c:1660  */
    {
    (yyval.node) = alloc_node("FunctionCall");
    add_attribute((yyval.node), "name", (yyvsp[-4].strval));
    add_child_list((yyval.node), "arguments", (yyvsp[-1].list));
  }
#line 2851 "parser.c" /* yacc.c:1660  */
    break;

  case 232:
#line 704 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "(*) in expression not yet supported"); }
#line 2857 "parser.c" /* yacc.c:1660  */
    break;

  case 233:
#line 705 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "OVER not yet supported"); }
#line 2863 "parser.c" /* yacc.c:1660  */
    break;

  case 234:
#line 706 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "OVER not yet supported"); }
#line 2869 "parser.c" /* yacc.c:1660  */
    break;

  case 235:
#line 707 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "expression lists not yet supported"); }
#line 2875 "parser.c" /* yacc.c:1660  */
    break;

  case 236:
#line 708 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "AND in expression not yet supported"); }
#line 2881 "parser.c" /* yacc.c:1660  */
    break;

  case 237:
#line 709 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "OR in expression not yet supported"); }
#line 2887 "parser.c" /* yacc.c:1660  */
    break;

  case 238:
#line 710 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "< in expression not yet supported"); }
#line 2893 "parser.c" /* yacc.c:1660  */
    break;

  case 239:
#line 711 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "> in expression not yet supported"); }
#line 2899 "parser.c" /* yacc.c:1660  */
    break;

  case 240:
#line 712 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, ">= in expression not yet supported"); }
#line 2905 "parser.c" /* yacc.c:1660  */
    break;

  case 241:
#line 713 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "<= in expression not yet supported"); }
#line 2911 "parser.c" /* yacc.c:1660  */
    break;

  case 242:
#line 714 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "= in expression not yet supported"); }
#line 2917 "parser.c" /* yacc.c:1660  */
    break;

  case 243:
#line 715 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "<> in expression not yet supported"); }
#line 2923 "parser.c" /* yacc.c:1660  */
    break;

  case 244:
#line 716 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "& in expression not yet supported"); }
#line 2929 "parser.c" /* yacc.c:1660  */
    break;

  case 245:
#line 717 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "| in expression not yet supported"); }
#line 2935 "parser.c" /* yacc.c:1660  */
    break;

  case 246:
#line 718 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "<< in expression not yet supported"); }
#line 2941 "parser.c" /* yacc.c:1660  */
    break;

  case 247:
#line 719 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, ">> in expression not yet supported"); }
#line 2947 "parser.c" /* yacc.c:1660  */
    break;

  case 248:
#line 720 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "+ in expression not yet supported"); }
#line 2953 "parser.c" /* yacc.c:1660  */
    break;

  case 249:
#line 721 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "- in expression not yet supported"); }
#line 2959 "parser.c" /* yacc.c:1660  */
    break;

  case 250:
#line 722 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "* in expression not yet supported"); }
#line 2965 "parser.c" /* yacc.c:1660  */
    break;

  case 251:
#line 723 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "/ in expression not yet supported"); }
#line 2971 "parser.c" /* yacc.c:1660  */
    break;

  case 252:
#line 724 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "% in expression not yet supported"); }
#line 2977 "parser.c" /* yacc.c:1660  */
    break;

  case 253:
#line 725 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "|| in expression not yet supported"); }
#line 2983 "parser.c" /* yacc.c:1660  */
    break;

  case 254:
#line 726 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "LIKE in expression not yet supported"); }
#line 2989 "parser.c" /* yacc.c:1660  */
    break;

  case 255:
#line 727 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ESCAPE in expression not yet supported"); }
#line 2995 "parser.c" /* yacc.c:1660  */
    break;

  case 256:
#line 728 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "ISNULL in expression not yet supported"); }
#line 3001 "parser.c" /* yacc.c:1660  */
    break;

  case 257:
#line 729 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "NOTNULL in expression not yet supported"); }
#line 3007 "parser.c" /* yacc.c:1660  */
    break;

  case 258:
#line 730 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "NOT NULL in expression not yet supported"); }
#line 3013 "parser.c" /* yacc.c:1660  */
    break;

  case 259:
#line 731 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "IS in expression not yet supported"); }
#line 3019 "parser.c" /* yacc.c:1660  */
    break;

  case 260:
#line 732 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "NOT in expression not yet supported"); }
#line 3025 "parser.c" /* yacc.c:1660  */
    break;

  case 261:
#line 733 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "~ in expression not yet supported"); }
#line 3031 "parser.c" /* yacc.c:1660  */
    break;

  case 262:
#line 734 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "+ in expression not yet supported"); }
#line 3037 "parser.c" /* yacc.c:1660  */
    break;

  case 263:
#line 735 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "- in expression not yet supported"); }
#line 3043 "parser.c" /* yacc.c:1660  */
    break;

  case 264:
#line 736 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "BETWEEN in expression not yet supported"); }
#line 3049 "parser.c" /* yacc.c:1660  */
    break;

  case 265:
#line 737 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "IN in expression not yet supported"); }
#line 3055 "parser.c" /* yacc.c:1660  */
    break;

  case 266:
#line 738 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "SELECT in expression not yet supported"); }
#line 3061 "parser.c" /* yacc.c:1660  */
    break;

  case 267:
#line 739 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "IN in expression not yet supported"); }
#line 3067 "parser.c" /* yacc.c:1660  */
    break;

  case 268:
#line 740 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "IN in expression not yet supported"); }
#line 3073 "parser.c" /* yacc.c:1660  */
    break;

  case 269:
#line 741 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "EXISTS in expression not yet supported"); }
#line 3079 "parser.c" /* yacc.c:1660  */
    break;

  case 270:
#line 742 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "CASE in expression not yet supported"); }
#line 3085 "parser.c" /* yacc.c:1660  */
    break;

  case 271:
#line 743 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "RAISE in expression not yet supported"); }
#line 3091 "parser.c" /* yacc.c:1660  */
    break;

  case 272:
#line 744 "parser.y" /* yacc.c:1660  */
    { yyerror(NULL, scanner, "RAISE in expression not yet supported"); }
#line 3097 "parser.c" /* yacc.c:1660  */
    break;

  case 293:
#line 789 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = (yyvsp[0].list);
  }
#line 3105 "parser.c" /* yacc.c:1660  */
    break;

  case 294:
#line 792 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = alloc_array();
  }
#line 3113 "parser.c" /* yacc.c:1660  */
    break;

  case 295:
#line 798 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = (yyvsp[-2].list);
    add_last((yyval.list), (yyvsp[0].node));
  }
#line 3122 "parser.c" /* yacc.c:1660  */
    break;

  case 296:
#line 802 "parser.y" /* yacc.c:1660  */
    {
    (yyval.list) = alloc_array();
    add_last((yyval.list), (yyvsp[0].node));
  }
#line 3131 "parser.c" /* yacc.c:1660  */
    break;


#line 3135 "parser.c" /* yacc.c:1660  */
      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (parse_tree, scanner, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (parse_tree, scanner, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, parse_tree, scanner);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp, parse_tree, scanner);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (parse_tree, scanner, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, parse_tree, scanner);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[*yyssp], yyvsp, parse_tree, scanner);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
#line 1039 "parser.y" /* yacc.c:1903  */

