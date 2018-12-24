// A Bison parser, made by GNU Bison 3.2.

// Skeleton implementation for Bison LALR(1) parsers in C++

// Copyright (C) 2002-2015, 2018 Free Software Foundation, Inc.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// As a special exception, you may create a larger work that contains
// part or all of the Bison parser skeleton and distribute that work
// under terms of your choice, so long as that work isn't itself a
// parser generator using the skeleton or a modified version thereof
// as a parser skeleton.  Alternatively, if you modify or redistribute
// the parser skeleton itself, you may (at your option) remove this
// special exception, which will cause the skeleton and the resulting
// Bison output files to be licensed under the GNU General Public
// License without this special exception.

// This special exception was added by the Free Software Foundation in
// version 2.2 of Bison.

// Undocumented macros, especially those whose name start with YY_,
// are private implementation details.  Do not rely on them.





#include "parser.h"


// Unqualified %code blocks.
#line 27 "parser.y" // lalr1.cc:437

#include "ParserDriver.h"

#line 49 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:437


#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> // FIXME: INFRINGES ON USER NAME SPACE.
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

// Whether we are compiled with exception support.
#ifndef YY_EXCEPTIONS
# if defined __GNUC__ && !defined __EXCEPTIONS
#  define YY_EXCEPTIONS 0
# else
#  define YY_EXCEPTIONS 1
# endif
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K].location)
/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

# ifndef YYLLOC_DEFAULT
#  define YYLLOC_DEFAULT(Current, Rhs, N)                               \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).begin  = YYRHSLOC (Rhs, 1).begin;                   \
          (Current).end    = YYRHSLOC (Rhs, N).end;                     \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).begin = (Current).end = YYRHSLOC (Rhs, 0).end;      \
        }                                                               \
    while (/*CONSTCOND*/ false)
# endif


// Suppress unused-variable warnings by "using" E.
#define YYUSE(E) ((void) (E))

// Enable debugging if requested.
#if YYDEBUG

// A pseudo ostream that takes yydebug_ into account.
# define YYCDEBUG if (yydebug_) (*yycdebug_)

# define YY_SYMBOL_PRINT(Title, Symbol)         \
  do {                                          \
    if (yydebug_)                               \
    {                                           \
      *yycdebug_ << Title << ' ';               \
      yy_print_ (*yycdebug_, Symbol);           \
      *yycdebug_ << '\n';                       \
    }                                           \
  } while (false)

# define YY_REDUCE_PRINT(Rule)          \
  do {                                  \
    if (yydebug_)                       \
      yy_reduce_print_ (Rule);          \
  } while (false)

# define YY_STACK_PRINT()               \
  do {                                  \
    if (yydebug_)                       \
      yystack_print_ ();                \
  } while (false)

#else // !YYDEBUG

# define YYCDEBUG if (false) std::cerr
# define YY_SYMBOL_PRINT(Title, Symbol)  YYUSE (Symbol)
# define YY_REDUCE_PRINT(Rule)           static_cast<void> (0)
# define YY_STACK_PRINT()                static_cast<void> (0)

#endif // !YYDEBUG

#define yyerrok         (yyerrstatus_ = 0)
#define yyclearin       (yyla.clear ())

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYRECOVERING()  (!!yyerrstatus_)


namespace yy {
#line 144 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:512

  /* Return YYSTR after stripping away unnecessary quotes and
     backslashes, so that it's suitable for yyerror.  The heuristic is
     that double-quoting is unnecessary unless the string contains an
     apostrophe, a comma, or backslash (other than backslash-backslash).
     YYSTR is taken from yytname.  */
  std::string
  parser::yytnamerr_ (const char *yystr)
  {
    if (*yystr == '"')
      {
        std::string yyr = "";
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
              // Fall through.
            default:
              yyr += *yyp;
              break;

            case '"':
              return yyr;
            }
      do_not_strip_quotes: ;
      }

    return yystr;
  }


  /// Build a parser object.
  parser::parser (ParserDriver& drv_yyarg)
    :
#if YYDEBUG
      yydebug_ (false),
      yycdebug_ (&std::cerr),
#endif
      drv (drv_yyarg)
  {}

  parser::~parser ()
  {}


  /*---------------.
  | Symbol types.  |
  `---------------*/



  // by_state.
  parser::by_state::by_state ()
    : state (empty_state)
  {}

  parser::by_state::by_state (const by_state& other)
    : state (other.state)
  {}

  void
  parser::by_state::clear ()
  {
    state = empty_state;
  }

  void
  parser::by_state::move (by_state& that)
  {
    state = that.state;
    that.clear ();
  }

  parser::by_state::by_state (state_type s)
    : state (s)
  {}

  parser::symbol_number_type
  parser::by_state::type_get () const
  {
    if (state == empty_state)
      return empty_symbol;
    else
      return yystos_[state];
  }

  parser::stack_symbol_type::stack_symbol_type ()
  {}

  parser::stack_symbol_type::stack_symbol_type (YY_RVREF (stack_symbol_type) that)
    : super_type (YY_MOVE (that.state), YY_MOVE (that.location))
  {
    switch (that.type_get ())
    {
      case 163: // cmd
      case 196: // select
      case 197: // selectnowith
      case 199: // oneselect
      case 227: // expr
        value.YY_MOVE_OR_COPY< ParseNode > (YY_MOVE (that.value));
        break;

      case 69: // ID
      case 75: // INDEXED
      case 85: // JOIN_KW
      case 135: // STRING
      case 174: // nm
      case 279: // id
        value.YY_MOVE_OR_COPY< std::string > (YY_MOVE (that.value));
        break;

      case 202: // sclp
      case 203: // selcollist
      case 205: // from
      case 207: // seltablist
      case 218: // groupby_opt
      case 235: // exprlist
      case 236: // nexprlist
        value.YY_MOVE_OR_COPY< std::vector<ParseNode> > (YY_MOVE (that.value));
        break;

      default:
        break;
    }

#if defined __cplusplus && 201103L <= __cplusplus
    // that is emptied.
    that.state = empty_state;
#endif
  }

  parser::stack_symbol_type::stack_symbol_type (state_type s, YY_MOVE_REF (symbol_type) that)
    : super_type (s, YY_MOVE (that.location))
  {
    switch (that.type_get ())
    {
      case 163: // cmd
      case 196: // select
      case 197: // selectnowith
      case 199: // oneselect
      case 227: // expr
        value.move< ParseNode > (YY_MOVE (that.value));
        break;

      case 69: // ID
      case 75: // INDEXED
      case 85: // JOIN_KW
      case 135: // STRING
      case 174: // nm
      case 279: // id
        value.move< std::string > (YY_MOVE (that.value));
        break;

      case 202: // sclp
      case 203: // selcollist
      case 205: // from
      case 207: // seltablist
      case 218: // groupby_opt
      case 235: // exprlist
      case 236: // nexprlist
        value.move< std::vector<ParseNode> > (YY_MOVE (that.value));
        break;

      default:
        break;
    }

    // that is emptied.
    that.type = empty_symbol;
  }

#if defined __cplusplus && __cplusplus < 201103L
  parser::stack_symbol_type&
  parser::stack_symbol_type::operator= (stack_symbol_type& that)
  {
    state = that.state;
    switch (that.type_get ())
    {
      case 163: // cmd
      case 196: // select
      case 197: // selectnowith
      case 199: // oneselect
      case 227: // expr
        value.move< ParseNode > (that.value);
        break;

      case 69: // ID
      case 75: // INDEXED
      case 85: // JOIN_KW
      case 135: // STRING
      case 174: // nm
      case 279: // id
        value.move< std::string > (that.value);
        break;

      case 202: // sclp
      case 203: // selcollist
      case 205: // from
      case 207: // seltablist
      case 218: // groupby_opt
      case 235: // exprlist
      case 236: // nexprlist
        value.move< std::vector<ParseNode> > (that.value);
        break;

      default:
        break;
    }

    location = that.location;
    // that is emptied.
    that.state = empty_state;
    return *this;
  }
#endif

  template <typename Base>
  void
  parser::yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const
  {
    if (yymsg)
      YY_SYMBOL_PRINT (yymsg, yysym);
  }

#if YYDEBUG
  template <typename Base>
  void
  parser::yy_print_ (std::ostream& yyo,
                                     const basic_symbol<Base>& yysym) const
  {
    std::ostream& yyoutput = yyo;
    YYUSE (yyoutput);
    symbol_number_type yytype = yysym.type_get ();
    // Avoid a (spurious) G++ 4.8 warning about "array subscript is
    // below array bounds".
    if (yysym.empty ())
      std::abort ();
    yyo << (yytype < yyntokens_ ? "token" : "nterm")
        << ' ' << yytname_[yytype] << " ("
        << yysym.location << ": ";
    YYUSE (yytype);
    yyo << ')';
  }
#endif

  void
  parser::yypush_ (const char* m, YY_MOVE_REF (stack_symbol_type) sym)
  {
    if (m)
      YY_SYMBOL_PRINT (m, sym);
    yystack_.push (YY_MOVE (sym));
  }

  void
  parser::yypush_ (const char* m, state_type s, YY_MOVE_REF (symbol_type) sym)
  {
#if defined __cplusplus && 201103L <= __cplusplus
    yypush_ (m, stack_symbol_type (s, std::move (sym)));
#else
    stack_symbol_type ss (s, sym);
    yypush_ (m, ss);
#endif
  }

  void
  parser::yypop_ (int n)
  {
    yystack_.pop (n);
  }

#if YYDEBUG
  std::ostream&
  parser::debug_stream () const
  {
    return *yycdebug_;
  }

  void
  parser::set_debug_stream (std::ostream& o)
  {
    yycdebug_ = &o;
  }


  parser::debug_level_type
  parser::debug_level () const
  {
    return yydebug_;
  }

  void
  parser::set_debug_level (debug_level_type l)
  {
    yydebug_ = l;
  }
#endif // YYDEBUG

  parser::state_type
  parser::yy_lr_goto_state_ (state_type yystate, int yysym)
  {
    int yyr = yypgoto_[yysym - yyntokens_] + yystate;
    if (0 <= yyr && yyr <= yylast_ && yycheck_[yyr] == yystate)
      return yytable_[yyr];
    else
      return yydefgoto_[yysym - yyntokens_];
  }

  bool
  parser::yy_pact_value_is_default_ (int yyvalue)
  {
    return yyvalue == yypact_ninf_;
  }

  bool
  parser::yy_table_value_is_error_ (int yyvalue)
  {
    return yyvalue == yytable_ninf_;
  }

  int
  parser::operator() ()
  {
    return parse ();
  }

  int
  parser::parse ()
  {
    // State.
    int yyn;
    /// Length of the RHS of the rule being reduced.
    int yylen = 0;

    // Error handling.
    int yynerrs_ = 0;
    int yyerrstatus_ = 0;

    /// The lookahead symbol.
    symbol_type yyla;

    /// The locations where the error started and ended.
    stack_symbol_type yyerror_range[3];

    /// The return value of parse ().
    int yyresult;

#if YY_EXCEPTIONS
    try
#endif // YY_EXCEPTIONS
      {
    YYCDEBUG << "Starting parse\n";


    /* Initialize the stack.  The initial state will be set in
       yynewstate, since the latter expects the semantical and the
       location values to have been already stored, initialize these
       stacks with a primary value.  */
    yystack_.clear ();
    yypush_ (YY_NULLPTR, 0, YY_MOVE (yyla));

    // A new symbol was pushed on the stack.
  yynewstate:
    YYCDEBUG << "Entering state " << yystack_[0].state << '\n';

    // Accept?
    if (yystack_[0].state == yyfinal_)
      goto yyacceptlab;

    goto yybackup;

    // Backup.
  yybackup:
    // Try to take a decision without lookahead.
    yyn = yypact_[yystack_[0].state];
    if (yy_pact_value_is_default_ (yyn))
      goto yydefault;

    // Read a lookahead token.
    if (yyla.empty ())
      {
        YYCDEBUG << "Reading a token: ";
#if YY_EXCEPTIONS
        try
#endif // YY_EXCEPTIONS
          {
            symbol_type yylookahead (yylex (drv));
            yyla.move (yylookahead);
          }
#if YY_EXCEPTIONS
        catch (const syntax_error& yyexc)
          {
            error (yyexc);
            goto yyerrlab1;
          }
#endif // YY_EXCEPTIONS
      }
    YY_SYMBOL_PRINT ("Next token is", yyla);

    /* If the proper action on seeing token YYLA.TYPE is to reduce or
       to detect an error, take that action.  */
    yyn += yyla.type_get ();
    if (yyn < 0 || yylast_ < yyn || yycheck_[yyn] != yyla.type_get ())
      goto yydefault;

    // Reduce or error.
    yyn = yytable_[yyn];
    if (yyn <= 0)
      {
        if (yy_table_value_is_error_ (yyn))
          goto yyerrlab;
        yyn = -yyn;
        goto yyreduce;
      }

    // Count tokens shifted since error; after three, turn off error status.
    if (yyerrstatus_)
      --yyerrstatus_;

    // Shift the lookahead token.
    yypush_ ("Shifting", yyn, YY_MOVE (yyla));
    goto yynewstate;

  /*-----------------------------------------------------------.
  | yydefault -- do the default action for the current state.  |
  `-----------------------------------------------------------*/
  yydefault:
    yyn = yydefact_[yystack_[0].state];
    if (yyn == 0)
      goto yyerrlab;
    goto yyreduce;

  /*-----------------------------.
  | yyreduce -- Do a reduction.  |
  `-----------------------------*/
  yyreduce:
    yylen = yyr2_[yyn];
    {
      stack_symbol_type yylhs;
      yylhs.state = yy_lr_goto_state_ (yystack_[yylen].state, yyr1_[yyn]);
      /* Variants are always initialized to an empty instance of the
         correct type. The default '$$ = $1' action is NOT applied
         when using variants.  */
      switch (yyr1_[yyn])
    {
      case 163: // cmd
      case 196: // select
      case 197: // selectnowith
      case 199: // oneselect
      case 227: // expr
        yylhs.value.emplace< ParseNode > ();
        break;

      case 69: // ID
      case 75: // INDEXED
      case 85: // JOIN_KW
      case 135: // STRING
      case 174: // nm
      case 279: // id
        yylhs.value.emplace< std::string > ();
        break;

      case 202: // sclp
      case 203: // selcollist
      case 205: // from
      case 207: // seltablist
      case 218: // groupby_opt
      case 235: // exprlist
      case 236: // nexprlist
        yylhs.value.emplace< std::vector<ParseNode> > ();
        break;

      default:
        break;
    }


      // Default location.
      {
        slice<stack_symbol_type, stack_type> slice (yystack_, yylen);
        YYLLOC_DEFAULT (yylhs.location, slice, yylen);
        yyerror_range[1].location = yylhs.location;
      }

      // Perform the reduction.
      YY_REDUCE_PRINT (yyn);
#if YY_EXCEPTIONS
      try
#endif // YY_EXCEPTIONS
        {
          switch (yyn)
            {
  case 10:
#line 241 "parser.y" // lalr1.cc:906
    { drv.syntax_tree = &yystack_[0].value.as< ParseNode > (); }
#line 647 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 11:
#line 245 "parser.y" // lalr1.cc:906
    { error(drv.location, "BEGIN not yet supported"); }
#line 653 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 12:
#line 246 "parser.y" // lalr1.cc:906
    { error(drv.location, "COMMIT not yet supported"); }
#line 659 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 13:
#line 247 "parser.y" // lalr1.cc:906
    { error(drv.location, "END not yet supported"); }
#line 665 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 14:
#line 248 "parser.y" // lalr1.cc:906
    { error(drv.location, "ROLLBACK not yet supported"); }
#line 671 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 15:
#line 249 "parser.y" // lalr1.cc:906
    { error(drv.location, "SAVEPOINT not yet supported"); }
#line 677 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 16:
#line 250 "parser.y" // lalr1.cc:906
    { error(drv.location, "RELEASE not yet supported"); }
#line 683 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 17:
#line 251 "parser.y" // lalr1.cc:906
    { error(drv.location, "ROLLBACK not yet supported"); }
#line 689 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 18:
#line 252 "parser.y" // lalr1.cc:906
    { error(drv.location, "CREATE TABLE not yet supported"); }
#line 695 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 19:
#line 253 "parser.y" // lalr1.cc:906
    { error(drv.location, "DROP TABLE not yet supported"); }
#line 701 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 20:
#line 254 "parser.y" // lalr1.cc:906
    { error(drv.location, "CREATE VIEW not yet supported"); }
#line 707 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 21:
#line 255 "parser.y" // lalr1.cc:906
    { error(drv.location, "DROP VIEW not yet supported"); }
#line 713 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 22:
#line 256 "parser.y" // lalr1.cc:906
    { yylhs.value.as< ParseNode > () = yystack_[0].value.as< ParseNode > (); }
#line 719 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 23:
#line 257 "parser.y" // lalr1.cc:906
    { error(drv.location, "DELETE not yet supported"); }
#line 725 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 24:
#line 258 "parser.y" // lalr1.cc:906
    { error(drv.location, "UPDATE not yet supported"); }
#line 731 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 25:
#line 259 "parser.y" // lalr1.cc:906
    { error(drv.location, "INSERT not yet supported"); }
#line 737 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 26:
#line 260 "parser.y" // lalr1.cc:906
    { error(drv.location, "INSERT not yet supported"); }
#line 743 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 27:
#line 261 "parser.y" // lalr1.cc:906
    { error(drv.location, "CREATE INDEX not yet supported"); }
#line 749 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 28:
#line 262 "parser.y" // lalr1.cc:906
    { error(drv.location, "DROP INDEX not yet supported"); }
#line 755 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 29:
#line 263 "parser.y" // lalr1.cc:906
    { error(drv.location, "VACUUM not yet supported"); }
#line 761 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 30:
#line 264 "parser.y" // lalr1.cc:906
    { error(drv.location, "VACUUM not yet supported"); }
#line 767 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 31:
#line 265 "parser.y" // lalr1.cc:906
    { error(drv.location, "PRAGMA not yet supported"); }
#line 773 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 32:
#line 266 "parser.y" // lalr1.cc:906
    { error(drv.location, "PRAGMA not yet supported"); }
#line 779 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 33:
#line 267 "parser.y" // lalr1.cc:906
    { error(drv.location, "PRAGMA not yet supported"); }
#line 785 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 34:
#line 268 "parser.y" // lalr1.cc:906
    { error(drv.location, "PRAGMA not yet supported"); }
#line 791 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 35:
#line 269 "parser.y" // lalr1.cc:906
    { error(drv.location, "PRAGMA not yet supported"); }
#line 797 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 36:
#line 270 "parser.y" // lalr1.cc:906
    { error(drv.location, "CREATE BEGIN END not yet supported"); }
#line 803 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 37:
#line 271 "parser.y" // lalr1.cc:906
    { error(drv.location, "DROP TRIGGER not yet supported"); }
#line 809 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 38:
#line 272 "parser.y" // lalr1.cc:906
    { error(drv.location, "ATTACH not yet supported"); }
#line 815 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 39:
#line 273 "parser.y" // lalr1.cc:906
    { error(drv.location, "DETACH not yet supported"); }
#line 821 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 40:
#line 274 "parser.y" // lalr1.cc:906
    { error(drv.location, "REINDEX not yet supported"); }
#line 827 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 41:
#line 275 "parser.y" // lalr1.cc:906
    { error(drv.location, "REINDEX not yet supported"); }
#line 833 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 42:
#line 276 "parser.y" // lalr1.cc:906
    { error(drv.location, "ANALYZE not yet supported"); }
#line 839 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 43:
#line 277 "parser.y" // lalr1.cc:906
    { error(drv.location, "ANALYZE not yet supported"); }
#line 845 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 44:
#line 278 "parser.y" // lalr1.cc:906
    { error(drv.location, "ALTER TABLE not yet supported"); }
#line 851 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 45:
#line 279 "parser.y" // lalr1.cc:906
    { error(drv.location, "ALTER TABLE not yet supported"); }
#line 857 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 46:
#line 280 "parser.y" // lalr1.cc:906
    { error(drv.location, "ALTER TABLE not yet supported"); }
#line 863 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 47:
#line 281 "parser.y" // lalr1.cc:906
    { error(drv.location, "VIRTUAL TABLE not yet supported"); }
#line 869 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 48:
#line 282 "parser.y" // lalr1.cc:906
    { error(drv.location, "VIRTUAL TABLE not yet supported"); }
#line 875 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 70:
#line 337 "parser.y" // lalr1.cc:906
    { yylhs.value.as< std::string > () = yystack_[0].value.as< std::string > (); }
#line 881 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 71:
#line 338 "parser.y" // lalr1.cc:906
    { yylhs.value.as< std::string > () = yystack_[0].value.as< std::string > (); }
#line 887 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 72:
#line 339 "parser.y" // lalr1.cc:906
    { yylhs.value.as< std::string > () = yystack_[0].value.as< std::string > (); }
#line 893 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 138:
#line 471 "parser.y" // lalr1.cc:906
    { error(drv.location, "WITH SELECT not yet supported"); }
#line 899 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 139:
#line 472 "parser.y" // lalr1.cc:906
    { error(drv.location, "WITH RECURSIVE SELECT not yet supported"); }
#line 905 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 140:
#line 473 "parser.y" // lalr1.cc:906
    { yylhs.value.as< ParseNode > () = yystack_[0].value.as< ParseNode > (); }
#line 911 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 141:
#line 477 "parser.y" // lalr1.cc:906
    { yylhs.value.as< ParseNode > () = yystack_[0].value.as< ParseNode > (); }
#line 917 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 142:
#line 478 "parser.y" // lalr1.cc:906
    { error(drv.location, "multiselect not yet supported"); }
#line 923 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 147:
#line 489 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< ParseNode > () = SelectNode(&yystack_[6].value.as< std::vector<ParseNode> > (), &yystack_[5].value.as< std::vector<ParseNode> > (), &yystack_[3].value.as< std::vector<ParseNode> > ());
  }
#line 931 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 148:
#line 492 "parser.y" // lalr1.cc:906
    { error(drv.location, "window queries not yet supported"); }
#line 937 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 149:
#line 493 "parser.y" // lalr1.cc:906
    { error(drv.location, "VALUES not yet supported"); }
#line 943 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 155:
#line 508 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > () = yystack_[1].value.as< std::vector<ParseNode> > ();
  }
#line 951 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 156:
#line 511 "parser.y" // lalr1.cc:906
    {}
#line 957 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 157:
#line 515 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > () = yystack_[4].value.as< std::vector<ParseNode> > ();
    yylhs.value.as< std::vector<ParseNode> > ().push_back(yystack_[2].value.as< ParseNode > ());
  }
#line 966 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 158:
#line 519 "parser.y" // lalr1.cc:906
    { error(drv.location, "SELECT *  not yet supported"); }
#line 972 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 159:
#line 520 "parser.y" // lalr1.cc:906
    { error(drv.location, "SELECT .* not yet supported"); }
#line 978 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 163:
#line 530 "parser.y" // lalr1.cc:906
    {}
#line 984 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 164:
#line 531 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > () = yystack_[0].value.as< std::vector<ParseNode> > ();
  }
#line 992 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 167:
#line 542 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > ().push_back(ReferenceNode(yystack_[5].value.as< std::string > ()));
  }
#line 1000 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 168:
#line 545 "parser.y" // lalr1.cc:906
    { error(drv.location, "parentheses in FROM clause not yet supported"); }
#line 1006 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 169:
#line 546 "parser.y" // lalr1.cc:906
    { error(drv.location, "nested select not yet supported"); }
#line 1012 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 170:
#line 547 "parser.y" // lalr1.cc:906
    { error(drv.location, "parentheses in FROM clause not yet supported"); }
#line 1018 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 198:
#line 608 "parser.y" // lalr1.cc:906
    {}
#line 1024 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 199:
#line 609 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > () = yystack_[0].value.as< std::vector<ParseNode> > ();
  }
#line 1032 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 222:
#line 661 "parser.y" // lalr1.cc:906
    { error(drv.location, "expression terms not yet supported"); }
#line 1038 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 223:
#line 662 "parser.y" // lalr1.cc:906
    { error(drv.location, "parentheses in expression not yet supported"); }
#line 1044 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 224:
#line 663 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< ParseNode > () = ReferenceNode(yystack_[0].value.as< std::string > ());
  }
#line 1052 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 225:
#line 666 "parser.y" // lalr1.cc:906
    { error(drv.location, "join keyword in expression not yet supported"); }
#line 1058 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 226:
#line 667 "parser.y" // lalr1.cc:906
    { error(drv.location, "nm.nm in expression not yet supported"); }
#line 1064 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 227:
#line 668 "parser.y" // lalr1.cc:906
    { error(drv.location, "nm.nm.nm in expression not yet supported"); }
#line 1070 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 228:
#line 669 "parser.y" // lalr1.cc:906
    { error(drv.location, "VARIABLE not yet supported"); }
#line 1076 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 229:
#line 670 "parser.y" // lalr1.cc:906
    { error(drv.location, "COLLATE not yet supported"); }
#line 1082 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 230:
#line 671 "parser.y" // lalr1.cc:906
    { error(drv.location, "CAST not yet supported"); }
#line 1088 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 231:
#line 672 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< ParseNode > () = FunctionNode(yystack_[4].value.as< std::string > (), &yystack_[1].value.as< std::vector<ParseNode> > ());
  }
#line 1096 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 232:
#line 675 "parser.y" // lalr1.cc:906
    { error(drv.location, "(*) in expression not yet supported"); }
#line 1102 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 233:
#line 676 "parser.y" // lalr1.cc:906
    { error(drv.location, "OVER not yet supported"); }
#line 1108 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 234:
#line 677 "parser.y" // lalr1.cc:906
    { error(drv.location, "OVER not yet supported"); }
#line 1114 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 235:
#line 678 "parser.y" // lalr1.cc:906
    { error(drv.location, "expression lists not yet supported"); }
#line 1120 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 236:
#line 679 "parser.y" // lalr1.cc:906
    { error(drv.location, "AND in expression not yet supported"); }
#line 1126 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 237:
#line 680 "parser.y" // lalr1.cc:906
    { error(drv.location, "OR in expression not yet supported"); }
#line 1132 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 238:
#line 681 "parser.y" // lalr1.cc:906
    { error(drv.location, "< in expression not yet supported"); }
#line 1138 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 239:
#line 682 "parser.y" // lalr1.cc:906
    { error(drv.location, "> in expression not yet supported"); }
#line 1144 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 240:
#line 683 "parser.y" // lalr1.cc:906
    { error(drv.location, ">= in expression not yet supported"); }
#line 1150 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 241:
#line 684 "parser.y" // lalr1.cc:906
    { error(drv.location, "<= in expression not yet supported"); }
#line 1156 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 242:
#line 685 "parser.y" // lalr1.cc:906
    { error(drv.location, "= in expression not yet supported"); }
#line 1162 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 243:
#line 686 "parser.y" // lalr1.cc:906
    { error(drv.location, "<> in expression not yet supported"); }
#line 1168 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 244:
#line 687 "parser.y" // lalr1.cc:906
    { error(drv.location, "& in expression not yet supported"); }
#line 1174 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 245:
#line 688 "parser.y" // lalr1.cc:906
    { error(drv.location, "| in expression not yet supported"); }
#line 1180 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 246:
#line 689 "parser.y" // lalr1.cc:906
    { error(drv.location, "<< in expression not yet supported"); }
#line 1186 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 247:
#line 690 "parser.y" // lalr1.cc:906
    { error(drv.location, ">> in expression not yet supported"); }
#line 1192 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 248:
#line 691 "parser.y" // lalr1.cc:906
    { error(drv.location, "+ in expression not yet supported"); }
#line 1198 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 249:
#line 692 "parser.y" // lalr1.cc:906
    { error(drv.location, "- in expression not yet supported"); }
#line 1204 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 250:
#line 693 "parser.y" // lalr1.cc:906
    { error(drv.location, "* in expression not yet supported"); }
#line 1210 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 251:
#line 694 "parser.y" // lalr1.cc:906
    { error(drv.location, "/ in expression not yet supported"); }
#line 1216 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 252:
#line 695 "parser.y" // lalr1.cc:906
    { error(drv.location, "% in expression not yet supported"); }
#line 1222 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 253:
#line 696 "parser.y" // lalr1.cc:906
    { error(drv.location, "|| in expression not yet supported"); }
#line 1228 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 254:
#line 697 "parser.y" // lalr1.cc:906
    { error(drv.location, "LIKE in expression not yet supported"); }
#line 1234 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 255:
#line 698 "parser.y" // lalr1.cc:906
    { error(drv.location, "ESCAPE in expression not yet supported"); }
#line 1240 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 256:
#line 699 "parser.y" // lalr1.cc:906
    { error(drv.location, "ISNULL in expression not yet supported"); }
#line 1246 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 257:
#line 700 "parser.y" // lalr1.cc:906
    { error(drv.location, "NOTNULL in expression not yet supported"); }
#line 1252 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 258:
#line 701 "parser.y" // lalr1.cc:906
    { error(drv.location, "NOT NULL in expression not yet supported"); }
#line 1258 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 259:
#line 702 "parser.y" // lalr1.cc:906
    { error(drv.location, "IS in expression not yet supported"); }
#line 1264 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 260:
#line 703 "parser.y" // lalr1.cc:906
    { error(drv.location, "NOT in expression not yet supported"); }
#line 1270 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 261:
#line 704 "parser.y" // lalr1.cc:906
    { error(drv.location, "~ in expression not yet supported"); }
#line 1276 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 262:
#line 705 "parser.y" // lalr1.cc:906
    { error(drv.location, "+ in expression not yet supported"); }
#line 1282 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 263:
#line 706 "parser.y" // lalr1.cc:906
    { error(drv.location, "- in expression not yet supported"); }
#line 1288 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 264:
#line 707 "parser.y" // lalr1.cc:906
    { error(drv.location, "BETWEEN in expression not yet supported"); }
#line 1294 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 265:
#line 708 "parser.y" // lalr1.cc:906
    { error(drv.location, "IN in expression not yet supported"); }
#line 1300 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 266:
#line 709 "parser.y" // lalr1.cc:906
    { error(drv.location, "SELECT in expression not yet supported"); }
#line 1306 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 267:
#line 710 "parser.y" // lalr1.cc:906
    { error(drv.location, "IN in expression not yet supported"); }
#line 1312 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 268:
#line 711 "parser.y" // lalr1.cc:906
    { error(drv.location, "IN in expression not yet supported"); }
#line 1318 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 269:
#line 712 "parser.y" // lalr1.cc:906
    { error(drv.location, "EXISTS in expression not yet supported"); }
#line 1324 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 270:
#line 713 "parser.y" // lalr1.cc:906
    { error(drv.location, "CASE in expression not yet supported"); }
#line 1330 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 271:
#line 714 "parser.y" // lalr1.cc:906
    { error(drv.location, "RAISE in expression not yet supported"); }
#line 1336 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 272:
#line 715 "parser.y" // lalr1.cc:906
    { error(drv.location, "RAISE in expression not yet supported"); }
#line 1342 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 293:
#line 760 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > () = yystack_[0].value.as< std::vector<ParseNode> > ();
  }
#line 1350 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 294:
#line 763 "parser.y" // lalr1.cc:906
    {}
#line 1356 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 295:
#line 767 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > () = yystack_[2].value.as< std::vector<ParseNode> > ();
    yylhs.value.as< std::vector<ParseNode> > ().push_back(yystack_[0].value.as< ParseNode > ());
  }
#line 1365 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 296:
#line 771 "parser.y" // lalr1.cc:906
    {
    yylhs.value.as< std::vector<ParseNode> > ().push_back(yystack_[0].value.as< ParseNode > ());
  }
#line 1373 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 388:
#line 994 "parser.y" // lalr1.cc:906
    { yylhs.value.as< std::string > () = yystack_[0].value.as< std::string > (); }
#line 1379 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;

  case 389:
#line 995 "parser.y" // lalr1.cc:906
    { yylhs.value.as< std::string > () = yystack_[0].value.as< std::string > (); }
#line 1385 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
    break;


#line 1389 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:906
            default:
              break;
            }
        }
#if YY_EXCEPTIONS
      catch (const syntax_error& yyexc)
        {
          error (yyexc);
          YYERROR;
        }
#endif // YY_EXCEPTIONS
      YY_SYMBOL_PRINT ("-> $$ =", yylhs);
      yypop_ (yylen);
      yylen = 0;
      YY_STACK_PRINT ();

      // Shift the result of the reduction.
      yypush_ (YY_NULLPTR, YY_MOVE (yylhs));
    }
    goto yynewstate;

  /*--------------------------------------.
  | yyerrlab -- here on detecting error.  |
  `--------------------------------------*/
  yyerrlab:
    // If not already recovering from an error, report this error.
    if (!yyerrstatus_)
      {
        ++yynerrs_;
        error (yyla.location, yysyntax_error_ (yystack_[0].state, yyla));
      }


    yyerror_range[1].location = yyla.location;
    if (yyerrstatus_ == 3)
      {
        /* If just tried and failed to reuse lookahead token after an
           error, discard it.  */

        // Return failure if at end of input.
        if (yyla.type_get () == yyeof_)
          YYABORT;
        else if (!yyla.empty ())
          {
            yy_destroy_ ("Error: discarding", yyla);
            yyla.clear ();
          }
      }

    // Else will try to reuse lookahead token after shifting the error token.
    goto yyerrlab1;


  /*---------------------------------------------------.
  | yyerrorlab -- error raised explicitly by YYERROR.  |
  `---------------------------------------------------*/
  yyerrorlab:

    /* Pacify compilers like GCC when the user code never invokes
       YYERROR and the label yyerrorlab therefore never appears in user
       code.  */
    if (false)
      goto yyerrorlab;
    /* Do not reclaim the symbols of the rule whose action triggered
       this YYERROR.  */
    yypop_ (yylen);
    yylen = 0;
    goto yyerrlab1;

  /*-------------------------------------------------------------.
  | yyerrlab1 -- common code for both syntax error and YYERROR.  |
  `-------------------------------------------------------------*/
  yyerrlab1:
    yyerrstatus_ = 3;   // Each real token shifted decrements this.
    {
      stack_symbol_type error_token;
      for (;;)
        {
          yyn = yypact_[yystack_[0].state];
          if (!yy_pact_value_is_default_ (yyn))
            {
              yyn += yyterror_;
              if (0 <= yyn && yyn <= yylast_ && yycheck_[yyn] == yyterror_)
                {
                  yyn = yytable_[yyn];
                  if (0 < yyn)
                    break;
                }
            }

          // Pop the current state because it cannot handle the error token.
          if (yystack_.size () == 1)
            YYABORT;

          yyerror_range[1].location = yystack_[0].location;
          yy_destroy_ ("Error: popping", yystack_[0]);
          yypop_ ();
          YY_STACK_PRINT ();
        }

      yyerror_range[2].location = yyla.location;
      YYLLOC_DEFAULT (error_token.location, yyerror_range, 2);

      // Shift the error token.
      error_token.state = yyn;
      yypush_ ("Shifting", YY_MOVE (error_token));
    }
    goto yynewstate;

    // Accept.
  yyacceptlab:
    yyresult = 0;
    goto yyreturn;

    // Abort.
  yyabortlab:
    yyresult = 1;
    goto yyreturn;

  yyreturn:
    if (!yyla.empty ())
      yy_destroy_ ("Cleanup: discarding lookahead", yyla);

    /* Do not reclaim the symbols of the rule whose action triggered
       this YYABORT or YYACCEPT.  */
    yypop_ (yylen);
    while (1 < yystack_.size ())
      {
        yy_destroy_ ("Cleanup: popping", yystack_[0]);
        yypop_ ();
      }

    return yyresult;
  }
#if YY_EXCEPTIONS
    catch (...)
      {
        YYCDEBUG << "Exception caught: cleaning lookahead and stack\n";
        // Do not try to display the values of the reclaimed symbols,
        // as their printers might throw an exception.
        if (!yyla.empty ())
          yy_destroy_ (YY_NULLPTR, yyla);

        while (1 < yystack_.size ())
          {
            yy_destroy_ (YY_NULLPTR, yystack_[0]);
            yypop_ ();
          }
        throw;
      }
#endif // YY_EXCEPTIONS
  }

  void
  parser::error (const syntax_error& yyexc)
  {
    error (yyexc.location, yyexc.what ());
  }

  // Generate an error message.
  std::string
  parser::yysyntax_error_ (state_type yystate, const symbol_type& yyla) const
  {
    // Number of reported tokens (one for the "unexpected", one per
    // "expected").
    size_t yycount = 0;
    // Its maximum.
    enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
    // Arguments of yyformat.
    char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];

    /* There are many possibilities here to consider:
       - If this state is a consistent state with a default action, then
         the only way this function was invoked is if the default action
         is an error action.  In that case, don't check for expected
         tokens because there are none.
       - The only way there can be no lookahead present (in yyla) is
         if this state is a consistent state with a default action.
         Thus, detecting the absence of a lookahead is sufficient to
         determine that there is no unexpected or expected token to
         report.  In that case, just report a simple "syntax error".
       - Don't assume there isn't a lookahead just because this state is
         a consistent state with a default action.  There might have
         been a previous inconsistent state, consistent state with a
         non-default action, or user semantic action that manipulated
         yyla.  (However, yyla is currently not documented for users.)
       - Of course, the expected token list depends on states to have
         correct lookahead information, and it depends on the parser not
         to perform extra reductions after fetching a lookahead from the
         scanner and before detecting a syntax error.  Thus, state
         merging (from LALR or IELR) and default reductions corrupt the
         expected token list.  However, the list is correct for
         canonical LR with one exception: it will still contain any
         token that will not be accepted due to an error action in a
         later state.
    */
    if (!yyla.empty ())
      {
        int yytoken = yyla.type_get ();
        yyarg[yycount++] = yytname_[yytoken];
        int yyn = yypact_[yystate];
        if (!yy_pact_value_is_default_ (yyn))
          {
            /* Start YYX at -YYN if negative to avoid negative indexes in
               YYCHECK.  In other words, skip the first -YYN actions for
               this state because they are default actions.  */
            int yyxbegin = yyn < 0 ? -yyn : 0;
            // Stay within bounds of both yycheck and yytname.
            int yychecklim = yylast_ - yyn + 1;
            int yyxend = yychecklim < yyntokens_ ? yychecklim : yyntokens_;
            for (int yyx = yyxbegin; yyx < yyxend; ++yyx)
              if (yycheck_[yyx + yyn] == yyx && yyx != yyterror_
                  && !yy_table_value_is_error_ (yytable_[yyx + yyn]))
                {
                  if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                    {
                      yycount = 1;
                      break;
                    }
                  else
                    yyarg[yycount++] = yytname_[yyx];
                }
          }
      }

    char const* yyformat = YY_NULLPTR;
    switch (yycount)
      {
#define YYCASE_(N, S)                         \
        case N:                               \
          yyformat = S;                       \
        break
      default: // Avoid compiler warnings.
        YYCASE_ (0, YY_("syntax error"));
        YYCASE_ (1, YY_("syntax error, unexpected %s"));
        YYCASE_ (2, YY_("syntax error, unexpected %s, expecting %s"));
        YYCASE_ (3, YY_("syntax error, unexpected %s, expecting %s or %s"));
        YYCASE_ (4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
        YYCASE_ (5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
      }

    std::string yyres;
    // Argument number.
    size_t yyi = 0;
    for (char const* yyp = yyformat; *yyp; ++yyp)
      if (yyp[0] == '%' && yyp[1] == 's' && yyi < yycount)
        {
          yyres += yytnamerr_ (yyarg[yyi++]);
          ++yyp;
        }
      else
        yyres += *yyp;
    return yyres;
  }


  const short parser::yypact_ninf_ = -568;

  const short parser::yytable_ninf_ = -387;

  const short
  parser::yypact_[] =
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

  const unsigned short
  parser::yydefact_[] =
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

  const short
  parser::yypgoto_[] =
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

  const short
  parser::yydefgoto_[] =
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

  const short
  parser::yytable_[] =
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

  const short
  parser::yycheck_[] =
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

  const unsigned short
  parser::yystos_[] =
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

  const unsigned short
  parser::yyr1_[] =
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

  const unsigned char
  parser::yyr2_[] =
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



  // YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
  // First, the terminals, then, starting at \a yyntokens_, nonterminals.
  const char*
  const parser::yytname_[] =
  {
  "EOF", "error", "$undefined", "ABORT", "ACTION", "ADD", "AFTER", "ALL",
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

#if YYDEBUG
  const unsigned short
  parser::yyrline_[] =
  {
       0,   221,   221,   225,   226,   230,   231,   232,   236,   237,
     241,   245,   246,   247,   248,   249,   250,   251,   252,   253,
     254,   255,   256,   257,   258,   259,   260,   261,   262,   263,
     264,   265,   266,   267,   268,   269,   270,   271,   272,   273,
     274,   275,   276,   277,   278,   279,   280,   281,   282,   285,
     287,   288,   291,   293,   294,   295,   299,   300,   304,   307,
     309,   313,   314,   318,   319,   322,   324,   328,   329,   333,
     337,   338,   339,   342,   344,   345,   346,   350,   351,   355,
     356,   359,   364,   365,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   385,   387,
     390,   392,   396,   397,   398,   399,   403,   404,   405,   406,
     407,   411,   412,   415,   417,   418,   421,   423,   427,   428,
     432,   433,   437,   438,   439,   440,   441,   444,   446,   449,
     451,   454,   456,   460,   461,   462,   466,   467,   471,   472,
     473,   477,   478,   482,   483,   484,   485,   489,   492,   493,
     497,   498,   502,   503,   504,   508,   511,   515,   519,   520,
     524,   525,   526,   530,   531,   537,   538,   542,   545,   546,
     547,   550,   552,   556,   557,   561,   562,   563,   564,   568,
     569,   570,   571,   572,   576,   577,   580,   582,   583,   587,
     588,   591,   593,   597,   598,   602,   603,   604,   608,   609,
     614,   616,   619,   621,   622,   623,   626,   628,   632,   633,
     634,   635,   638,   640,   641,   642,   646,   647,   650,   652,
     656,   657,   661,   662,   663,   666,   667,   668,   669,   670,
     671,   672,   675,   676,   677,   678,   679,   680,   681,   682,
     683,   684,   685,   686,   687,   688,   689,   690,   691,   692,
     693,   694,   695,   696,   697,   698,   699,   700,   701,   702,
     703,   704,   705,   706,   707,   708,   709,   710,   711,   712,
     713,   714,   715,   719,   720,   721,   722,   723,   724,   728,
     729,   730,   731,   735,   736,   740,   741,   745,   746,   750,
     751,   755,   756,   760,   763,   767,   771,   776,   778,   782,
     783,   786,   788,   792,   793,   796,   798,   802,   803,   804,
     805,   806,   810,   811,   815,   819,   823,   824,   825,   826,
     830,   831,   832,   833,   836,   838,   841,   843,   847,   848,
     852,   853,   856,   858,   859,   863,   864,   865,   866,   870,
     871,   872,   875,   877,   881,   882,   886,   889,   891,   895,
     899,   900,   903,   905,   909,   910,   914,   917,   919,   920,
     923,   925,   926,   930,   931,   935,   936,   940,   944,   948,
     949,   952,   954,   955,   959,   960,   964,   965,   969,   970,
     974,   975,   976,   980,   984,   985,   988,   990,   994,   995,
     999,  1000,  1004,  1005
  };

  // Print the state stack on the debug stream.
  void
  parser::yystack_print_ ()
  {
    *yycdebug_ << "Stack now";
    for (stack_type::const_iterator
           i = yystack_.begin (),
           i_end = yystack_.end ();
         i != i_end; ++i)
      *yycdebug_ << ' ' << i->state;
    *yycdebug_ << '\n';
  }

  // Report on the debug stream that the rule \a yyrule is going to be reduced.
  void
  parser::yy_reduce_print_ (int yyrule)
  {
    unsigned yylno = yyrline_[yyrule];
    int yynrhs = yyr2_[yyrule];
    // Print the symbols being reduced, and their result.
    *yycdebug_ << "Reducing stack by rule " << yyrule - 1
               << " (line " << yylno << "):\n";
    // The symbols being reduced.
    for (int yyi = 0; yyi < yynrhs; yyi++)
      YY_SYMBOL_PRINT ("   $" << yyi + 1 << " =",
                       yystack_[(yynrhs) - (yyi + 1)]);
  }
#endif // YYDEBUG



} // yy
#line 2730 "/Users/kevingaffney/Dev/hustle/parser/parser.cpp" // lalr1.cc:1217
#line 1007 "parser.y" // lalr1.cc:1218


void yy::parser::error(const location_type& l, const std::string& m)
{
  std::cerr << l << ": " << m << '\n';
}
