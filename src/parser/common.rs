use nom::branch::alt;
use nom::bytes::complete::{escaped, tag, take_while, take_while1};
use nom::character::complete::{digit1, none_of};
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::{delimited, pair, tuple};
use nom::IResult;
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub enum Entity {
    Attribute,
    Tag,
    Param,
    Metric,
}

#[derive(Debug, PartialEq)]
pub struct Identifier {
    pub entity: Entity,
    pub key: String,
}

#[derive(Debug, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Literal::String(x) => write!(f, "'{}'", x),
            Literal::Integer(x) => write!(f, "{}", x),
            Literal::Float(x) => write!(f, "{}", x),
        }
    }
}

pub fn string_literal(i: &str) -> IResult<&str, Literal> {
    map(
        delimited(tag("'"), take_while(|c| c != '\''), tag("'")),
        |s: &str| Literal::String(s.to_string()),
    )(i)
}

pub fn integer_literal(i: &str) -> IResult<&str, Literal> {
    map(pair(opt(tag("-")), digit1), |tup| {
        let intval = i64::from_str(tup.1).unwrap();
        let sign = if (tup.0).is_some() { -1 } else { 1 };
        Literal::Integer(sign * intval)
    })(i)
}

pub fn float_literal(i: &str) -> IResult<&str, Literal> {
    map(tuple((opt(tag("-")), digit1, tag("."), digit1)), |tup| {
        let floatval = f64::from_str(format!("{}.{}", tup.1, tup.3).as_str()).unwrap();
        let sign = if (tup.0).is_some() { -1.0 } else { 1.0 };
        Literal::Float(sign * floatval)
    })(i)
}

pub fn literal(i: &str) -> IResult<&str, Literal> {
    alt((float_literal, integer_literal, string_literal))(i)
}

fn alphanumeric_or_underscore(i: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(i)
}

fn double_quote_delimited(input: &str) -> IResult<&str, &str> {
    let esc = escaped(none_of("\\\""), '\\', tag("\""));
    let esc_or_empty = alt((esc, tag("")));
    delimited(tag("\""), esc_or_empty, tag("\""))(input)
}

fn backtick_delimited(input: &str) -> IResult<&str, &str> {
    delimited(tag("`"), take_while1(|c: char| c != '`'), tag("`"))(input)
}

pub fn identifier(i: &str) -> IResult<&str, Identifier> {
    map(
        tuple((
            opt(alt((
                tag("attribute."),
                tag("param."),
                tag("metric."),
                tag("tag."),
            ))),
            alt((
                alphanumeric_or_underscore,
                double_quote_delimited,
                backtick_delimited,
            )),
        )),
        |tup: (Option<&str>, &str)| {
            let entity = match tup.0 {
                Some(s) => s.trim_end_matches(".").to_string(),
                None => "attribute".to_string(),
            };
            Identifier {
                entity: match entity.as_str() {
                    "attribute" => Entity::Attribute,
                    "param" => Entity::Param,
                    "metric" => Entity::Metric,
                    "tag" => Entity::Tag,
                    _ => unreachable!(),
                },
                key: tup.1.to_string(),
            }
        },
    )(i)
}

#[cfg(test)]
mod tests {
    use super::{
        alphanumeric_or_underscore, backtick_delimited, double_quote_delimited, float_literal,
        identifier, integer_literal, literal, string_literal, Entity, Identifier, Literal,
    };

    #[test]
    fn test_alphanumeric_or_underscore() {
        assert_eq!(alphanumeric_or_underscore("a").unwrap(), ("", "a"));
        assert_eq!(alphanumeric_or_underscore("a_b").unwrap(), ("", "a_b"));
        assert_eq!(
            alphanumeric_or_underscore("a_b = 'x'").unwrap(),
            (" = 'x'", "a_b")
        );
    }

    #[test]
    fn test_double_quote_delimited() {
        assert_eq!(double_quote_delimited("\"a\""), Ok(("", "a")));
        assert_eq!(double_quote_delimited("\"abc\""), Ok(("", "abc")));
        assert_eq!(double_quote_delimited("\"a b c\""), Ok(("", "a b c")));
        assert_eq!(
            double_quote_delimited("\"abc\" = 'value'"),
            Ok((" = 'value'", "abc"))
        );
    }

    #[test]
    fn test_backtick_delimited() {
        assert_eq!(backtick_delimited("`a`"), Ok(("", "a")));
        assert_eq!(backtick_delimited("`abc`"), Ok(("", "abc")));
        assert_eq!(backtick_delimited("`a b c`"), Ok(("", "a b c")));
        assert_eq!(
            backtick_delimited("`abc` = 'value'"),
            Ok((" = 'value'", "abc"))
        );
    }

    #[test]
    fn test_string_literal() {
        let res = string_literal("'hello'").unwrap();
        assert_eq!(res, ("", Literal::String("hello".to_string())));

        let res = string_literal("'he%'").unwrap();
        assert_eq!(res, ("", Literal::String("he%".to_string())));

        let res = string_literal("''").unwrap();
        assert_eq!(res, ("", Literal::String("".to_string())));
    }

    #[test]
    fn test_integer_literal() {
        let res = integer_literal("1").unwrap();
        assert_eq!(res, ("", Literal::Integer(1)));

        let res = integer_literal("100").unwrap();
        assert_eq!(res, ("", Literal::Integer(100)));

        let res = integer_literal("-1").unwrap();
        assert_eq!(res, ("", Literal::Integer(-1)));
    }

    #[test]
    fn test_float_literal() {
        let res = float_literal("0.1").unwrap();
        assert_eq!(res, ("", Literal::Float(0.1)));

        let res = float_literal("0.123").unwrap();
        assert_eq!(res, ("", Literal::Float(0.123)));

        let res = float_literal("-0.1").unwrap();
        assert_eq!(res, ("", Literal::Float(-0.1)));
    }

    #[test]
    fn test_literal() {
        let res = literal("1").unwrap();
        assert_eq!(res, ("", Literal::Integer(1)));
        let res = literal("1.0").unwrap();
        assert_eq!(res, ("", Literal::Float(1.0)));
        let res = literal("'string'").unwrap();
        assert_eq!(res, ("", Literal::String("string".to_string())));
    }

    #[test]
    fn test_identifier() {
        let res = identifier("key").unwrap();
        assert_eq!(
            res,
            (
                "",
                Identifier {
                    entity: Entity::Attribute,
                    key: "key".to_string()
                }
            )
        );

        let res = identifier("attribute.key").unwrap();
        assert_eq!(
            res,
            (
                "",
                Identifier {
                    entity: Entity::Attribute,
                    key: "key".to_string()
                }
            )
        );

        let res = identifier("attribute.\"key\"").unwrap();
        assert_eq!(
            res,
            (
                "",
                Identifier {
                    entity: Entity::Attribute,
                    key: "key".to_string()
                }
            )
        );

        let res = identifier("tag.key").unwrap();
        assert_eq!(
            res,
            (
                "",
                Identifier {
                    entity: Entity::Tag,
                    key: "key".to_string()
                }
            )
        );

        let res = identifier("param.key").unwrap();
        assert_eq!(
            res,
            (
                "",
                Identifier {
                    entity: Entity::Param,
                    key: "key".to_string()
                }
            )
        );

        let res = identifier("metric.key").unwrap();
        assert_eq!(
            res,
            (
                "",
                Identifier {
                    entity: Entity::Metric,
                    key: "key".to_string()
                }
            )
        );
    }
}
