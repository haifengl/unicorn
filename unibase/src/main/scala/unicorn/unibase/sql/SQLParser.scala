package unicorn.unibase.sql

import scala.util.matching.Regex
//import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
//import scala.util.parsing.combinator.token._
import scala.util.parsing.input.CharArrayReader.EofCh

/** A simple SQL parser.
  * Based on https://github.com/epfldata/dblab
  *
  * @author Haifeng Li
  */
object SQLParser extends StandardTokenParsers {

  def parse(statement: String): Option[SelectStatement] = {
    phrase(parseSelectStatement)(new lexical.Scanner(statement)) match {
      case Success(r, q) => Option(r)
      case _             => None
    }
  }

  def parseSelectStatement: Parser[SelectStatement] = {
    "SELECT" ~> parseProjections ~ "FROM" ~ parseRelations ~ parseWhere.? ~ parseGroupBy.? ~ parseOrderBy.? ~ parseLimit.? <~ ";".? ^^ { case pro ~ _ ~ tab ~ whe ~ grp ~ ord ~ lim => SelectStatement(pro, tab, whe, grp, ord, lim) }
  }

  def parseProjections: Parser[Projections] = {
    "*" ^^^ AllColumns() |
    rep1sep(parseAliasedExpression, ",") ^^ { case lst => ExpressionProjections(lst) }
  }

  def parseAliasedExpression: Parser[(Expression, Option[String])] = {
    parseExpression ~ ("AS" ~> ident).? ^^ { case expr ~ alias => (expr, alias) }
  }

  def parseExpression: Parser[Expression] = parseOr

  def parseOr: Parser[Expression] =
    parseAnd * ("OR" ^^^ { (a: Expression, b: Expression) => Or(a, b) })

  def parseAnd: Parser[Expression] =
    parseSimpleExpression * ("AND" ^^^ { (a: Expression, b: Expression) => And(a, b) })

  def parseSimpleExpression: Parser[Expression] = {
    parseAddition ~ rep(
      ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ parseAddition ^^ {
        case op ~ right => (op, right)
      } |
      "BETWEEN" ~ parseAddition ~ "AND" ~ parseAddition ^^ {
        case op ~ a ~ _ ~ b => (op, a, b)
      } |
      "NOT" ~> "IN" ~ "(" ~ (parseSelectStatement | rep1sep(parseExpression, ",")) ~ ")" ^^ {
        case op ~ _ ~ a ~ _ => (op, a, true)
      } |
      "IN" ~ "(" ~ (parseSelectStatement | rep1sep(parseExpression, ",")) ~ ")" ^^ {
        case op ~ _ ~ a ~ _ => (op, a, false)
      } |
      "NOT" ~> "LIKE" ~ parseAddition ^^ { case op ~ a => (op, a, true) } |
      "LIKE" ~ parseAddition ^^ { case op ~ a => (op, a, false) }) ^^ {
    case left ~ elems =>
      elems.foldLeft(left) {
        case (acc, (("=", right: Expression)))                  => Equals(acc, right)
        case (acc, (("<>", right: Expression)))                 => NotEquals(acc, right)
        case (acc, (("!=", right: Expression)))                 => NotEquals(acc, right)
        case (acc, (("<", right: Expression)))                  => LessThan(acc, right)
        case (acc, (("<=", right: Expression)))                 => LessOrEqual(acc, right)
        case (acc, ((">", right: Expression)))                  => GreaterThan(acc, right)
        case (acc, ((">=", right: Expression)))                 => GreaterOrEqual(acc, right)
        case (acc, (("BETWEEN", l: Expression, r: Expression))) => And(GreaterOrEqual(acc, l), LessOrEqual(acc, r))
        case (acc, (("IN", e: Seq[_], n: Boolean)))             => In(acc, e.asInstanceOf[Seq[Expression]], n)
        case (acc, (("IN", s: SelectStatement, n: Boolean)))    => In(acc, Seq(s), n)
        case (acc, (("LIKE", e: Expression, n: Boolean)))       => Like(acc, e, n)
      }
    } |
    "NOT" ~> parseSimpleExpression ^^ (Not(_)) |
    "EXISTS" ~> "(" ~> parseSelectStatement <~ ")" ^^ { case s => Exists(s) }
  }

  def parseAddition: Parser[Expression] =
    parseMultiplication * (
      "+" ^^^ { (a: Expression, b: Expression) => Add(a, b) } |
      "-" ^^^ { (a: Expression, b: Expression) => Subtract(a, b) })

  def parseMultiplication: Parser[Expression] =
    parsePrimaryExpression * (
      "*" ^^^ { (a: Expression, b: Expression) => Multiply(a, b) } |
      "/" ^^^ { (a: Expression, b: Expression) => Divide(a, b) })

  def parseJsonField: Parser[Expression] = {
    (dotNotation | ident) ^^ (FieldIdent(None, _))
  }

  val dotNotation: Parser[String] = {
    ident ~ (("." ~> ident) | ("[" ~> numericLit <~ "]")) ~ rep(("." ~> ident) | ("[" ~> numericLit <~ "]")) ^^ {
      case i1 ~ i2 ~ rest => i1 + "." + i2 + (if (rest.isEmpty) "" else rest.mkString(".", ".", ""))
    }
  }

  def parsePrimaryExpression: Parser[Expression] = {
    parseLiteral |
    parseKnownFunction |
    parseJsonField |
    /*
    // Use parseJsonField for fields but we cannot tell t.a.b where t is the table name or alias
    // https://mail-archives.apache.org/mod_mbox/spark-commits/201409.mbox/%3C667f345ef8b349a897f1321426c1a2ec@git.apache.org%3E
    ident ~ opt("." ~> ident | "(" ~> repsep(parseExpression, ",") <~ ")") ^^ {
      case id ~ None           => FieldIdent(None, id)
      case a ~ Some(b: String) => FieldIdent(Some(a), b)
    } |
    */
    "(" ~> (parseExpression | parseSelectStatement) <~ ")" |
    "+" ~> parsePrimaryExpression ^^ (UnaryPlus(_)) |
    "-" ~> parsePrimaryExpression ^^ (UnaryMinus(_))
  }

  def parseKnownFunction: Parser[Expression] = {
    "COUNT" ~> "(" ~> ("*" ^^^ CountAll() |
    parseExpression ^^ { case expr => CountExpr(expr) }) <~ ")" |
    "MIN" ~> "(" ~> parseExpression <~ ")" ^^ (Min(_)) |
    "MAX" ~> "(" ~> parseExpression <~ ")" ^^ (Max(_)) |
    "SUM" ~> "(" ~> parseExpression <~ ")" ^^ (Sum(_)) |
    "AVG" ~> "(" ~> parseExpression <~ ")" ^^ (Avg(_))
  }

  def parseLiteral: Parser[Expression] = {
    numericLit ^^ { case i => IntLiteral(i.toInt) } |
    floatLit ^^ { case f => FloatLiteral(f.toDouble) } |
    stringLit ^^ { case s => StringLiteral(s) } |
    "NULL" ^^ { case _ => NullLiteral() } |
    "DATE" ~> stringLit ^^ { case s => DateLiteral(s) }
  }

  def parseRelations: Parser[Seq[Relation]] = rep1sep(parseRelation, ",")

  def parseRelation: Parser[Relation] = {
    parseSimpleRelation ~ rep(opt(parseJoinType) ~ "JOIN" ~ parseSimpleRelation ~ "ON" ~ parseExpression ^^ {
      case tpe ~ _ ~ r ~ _ ~ e => (tpe.getOrElse(InnerJoin), r, e) }) ^^ {
        case r ~ elems => elems.foldLeft(r) { case (x, r) => Join(x, r._2, r._1, r._3)
      }
    }
  }

  def parseSimpleRelation: Parser[Relation] = {
    ident ~ ("AS".? ~> ident.?) ^^ {
      case tbl ~ alias => Table(tbl, alias)
    } |
    ("(" ~> parseSelectStatement <~ ")") ~ ("AS".? ~> ident) ^^ {
      case subq ~ alias => Subquery(subq, alias)
    }
  }

  def parseJoinType: Parser[JoinType] = {
    ("LEFT" <~ "OUTER".? | "RIGHT" <~ "OUTER".? | "FULL" ~ "OUTER") ^^ {
      case "LEFT"           => LeftOuterJoin
      case "RIGHT"          => RightOuterJoin
      case "FULL" ~ "OUTER" => FullOuterJoin
    } |
    "INNER" ^^^ InnerJoin
  }

  def parseWhere: Parser[Expression] = {
    "WHERE" ~> parseExpression
  }

  def parseGroupBy: Parser[GroupBy] = {
    "GROUP" ~> "BY" ~> rep1sep(parseExpression, ",") ~ ("HAVING" ~> parseExpression).? ^^ { case exp ~ hav => GroupBy(exp, hav) }
  }

  def parseOrderBy: Parser[OrderBy] = {
    "ORDER" ~> "BY" ~> rep1sep(parseOrderKey, ",") ^^ { case keys => OrderBy(keys) }
  }

  def parseOrderKey: Parser[(Expression, OrderType)] = {
    parseExpression ~ ("ASC" | "DESC").? ^^ {
      case v ~ Some("DESC") => (v, DESC)
      case v ~ Some("ASC")  => (v, ASC)
      case v ~ None         => (v, ASC)
    }
  }

  def parseLimit: Parser[Limit] = {
    "LIMIT" ~> numericLit ^^ { case lim => Limit(lim.toInt) }
  }

  def regex(r: Regex): Parser[String] = new Parser[String] {
    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      val start = offset // handleWhiteSpace(source, offset)
      (r findPrefixMatchOf (source.subSequence(start, source.length))) match {
        case Some(matched) =>
          Success(source.subSequence(start, start + matched.end).toString, in.drop(start + matched.end - offset))
        case None =>
          Success("", in)
      }
    }
  }

  class SqlLexical extends StdLexical {
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }

    reserved.clear
    reserved += (
      "SELECT", "AS", "OR", "AND", "GROUP", "ORDER", "BY", "WHERE",
      "JOIN", "ASC", "DESC", "FROM", "ON", "NOT", "HAVING",
      "EXISTS", "BETWEEN", "LIKE", "IN", "NULL", "LEFT", "RIGHT",
      "FULL", "OUTER", "INNER", "COUNT", "SUM", "AVG", "MIN", "MAX",
      "DATE", "TOP", "LIMIT")

    delimiters += (
      "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", "[", "]", ";")

    /* Normal the keyword string */
    def normalizeKeyword(str: String): String = str.toUpperCase

    // Allow case insensitive keywords by upper casing everything
    override protected def processIdent(name: String) = {
      val token = normalizeKeyword(name)
      if (reserved contains token) Keyword(token) else Identifier(name)
    }

    override def token: Parser[Token] = {
      identChar ~ rep(identChar | digit) ^^ { case first ~ rest => processIdent(first :: rest mkString "") } |
      rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
        case i ~ None    => NumericLit(i mkString "")
        case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
      } |
      '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") } |
      '\"' ~ rep(chrExcept('\"', '\n', EofCh)) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") } |
      EofCh ^^^ EOF |
      '\'' ~> failure("unclosed string literal") |
      '\"' ~> failure("unclosed string literal") |
      delim |
      failure("illegal character")
    }
  }

  override val lexical = new SqlLexical

  def floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)
}