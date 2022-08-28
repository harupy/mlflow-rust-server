use crate::parser::common::{identifier, literal, Identifier, Literal};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::multispace0;
use nom::combinator::map;
use nom::multi::separated_list0;
use nom::sequence::{delimited, tuple};
use nom::IResult;

#[derive(Debug, PartialEq)]
pub enum Comparator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    ILike,
}

impl std::fmt::Display for Comparator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Comparator::Equal => write!(f, "="),
            Comparator::NotEqual => write!(f, "!="),
            Comparator::GreaterThan => write!(f, ">"),
            Comparator::GreaterThanOrEqual => write!(f, ">="),
            Comparator::LessThan => write!(f, "<"),
            Comparator::LessThanOrEqual => write!(f, "<="),
            Comparator::Like => write!(f, "LIKE"),
            Comparator::ILike => write!(f, "ILIKE"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum LogicalOperator {
    And,
}

#[derive(Debug, PartialEq)]
pub struct Comparison {
    pub operator: Comparator,
    pub left: Identifier,
    pub right: Literal,
}

pub fn comparator(i: &str) -> IResult<&str, Comparator> {
    map(
        delimited(
            multispace0,
            alt((
                tag("!="),
                tag("<="),
                tag(">="),
                tag("<"),
                tag(">"),
                tag("="),
                tag_no_case("like"),
                tag_no_case("ilike"),
            )),
            multispace0,
        ),
        |c: &str| match c.to_lowercase().as_str() {
            "=" => Comparator::Equal,
            "!=" => Comparator::NotEqual,
            "<" => Comparator::LessThan,
            "<=" => Comparator::LessThanOrEqual,
            ">" => Comparator::GreaterThan,
            ">=" => Comparator::GreaterThanOrEqual,
            "like" => Comparator::Like,
            "ilike" => Comparator::ILike,
            _ => unreachable!(),
        },
    )(i)
}

pub fn comparison_op(i: &str) -> IResult<&str, Comparison> {
    map(
        tuple((identifier, comparator, literal)),
        |tup: (Identifier, Comparator, Literal)| Comparison {
            operator: tup.1,
            left: tup.0,
            right: tup.2,
        },
    )(i)
}

pub fn logical_operator(i: &str) -> IResult<&str, LogicalOperator> {
    map(
        delimited(multispace0, alt((tag_no_case("and"),)), multispace0),
        |c: &str| match c.to_lowercase().as_str() {
            "and" => LogicalOperator::And,
            _ => panic!("Unknown logical operator"),
        },
    )(i)
}

pub fn parse_filter(i: &str) -> IResult<&str, Vec<Comparison>> {
    separated_list0(logical_operator, comparison_op)(i)
}

#[cfg(test)]
mod tests {
    use super::{
        comparator, comparison_op, logical_operator, parse_filter, Comparator, Comparison,
        Identifier, Literal, LogicalOperator,
    };
    use crate::parser::common::Entity;

    #[test]
    fn test_comparator() {
        let res = comparator("=").unwrap();
        assert_eq!(res, ("", Comparator::Equal));

        let res = comparator(" =").unwrap();
        assert_eq!(res, ("", Comparator::Equal));

        let res = comparator("= ").unwrap();
        assert_eq!(res, ("", Comparator::Equal));

        let res = comparator(" = ").unwrap();
        assert_eq!(res, ("", Comparator::Equal));

        let res = comparator("!=").unwrap();
        assert_eq!(res, ("", Comparator::NotEqual));

        let res = comparator("<").unwrap();
        assert_eq!(res, ("", Comparator::LessThan));

        let res = comparator("<=").unwrap();
        assert_eq!(res, ("", Comparator::LessThanOrEqual));

        let res = comparator(">").unwrap();
        assert_eq!(res, ("", Comparator::GreaterThan));

        let res = comparator(">=").unwrap();
        assert_eq!(res, ("", Comparator::GreaterThanOrEqual));

        let res = comparator("LIKE").unwrap();
        assert_eq!(res, ("", Comparator::Like));

        let res = comparator("like").unwrap();
        assert_eq!(res, ("", Comparator::Like));

        let res = comparator("LiKe").unwrap();
        assert_eq!(res, ("", Comparator::Like));

        let res = comparator("ILIKE").unwrap();
        assert_eq!(res, ("", Comparator::ILike));
    }

    #[test]
    fn test_comparison_op() {
        let res = comparison_op("attribute.key = 'value'").unwrap();
        let expected = (
            "",
            Comparison {
                operator: Comparator::Equal,
                left: Identifier {
                    entity: Entity::Attribute,
                    key: "key".to_string(),
                },
                right: Literal::String("value".to_string()),
            },
        );
        assert_eq!(res, expected);

        let res = comparison_op("attribute.key='value'").unwrap();
        assert_eq!(res, expected);

        let res = comparison_op("attribute.\"key\" = 'value'").unwrap();
        assert_eq!(res, expected);
        let res = comparison_op("attribute.\"k e y\" = 'value'").unwrap();
        let expected_contains_spaces = (
            "",
            Comparison {
                operator: Comparator::Equal,
                left: Identifier {
                    entity: Entity::Attribute,
                    key: "k e y".to_string(),
                },
                right: Literal::String("value".to_string()),
            },
        );
        assert_eq!(res, expected_contains_spaces);
        let res = comparison_op("attribute.`key`='value'").unwrap();
        assert_eq!(res, expected);

        let res = comparison_op("attribute.`k e y` = 'value'").unwrap();
        assert_eq!(res, expected_contains_spaces);
    }

    #[test]
    fn test_logical_operator() {
        let res = logical_operator("and").unwrap();
        assert_eq!(res, ("", LogicalOperator::And));

        let res = logical_operator(" and").unwrap();
        assert_eq!(res, ("", LogicalOperator::And));

        let res = logical_operator("and ").unwrap();
        assert_eq!(res, ("", LogicalOperator::And));

        let res = logical_operator(" and ").unwrap();
        assert_eq!(res, ("", LogicalOperator::And));

        let res = logical_operator("AND").unwrap();
        assert_eq!(res, ("", LogicalOperator::And));

        let res = logical_operator("AnD").unwrap();
        assert_eq!(res, ("", LogicalOperator::And));
    }

    #[test]
    fn test_parse_comparisons() {
        let res = parse_filter("attribute.key = 'value'").unwrap();
        let expected = (
            "",
            vec![Comparison {
                operator: Comparator::Equal,
                left: Identifier {
                    entity: Entity::Attribute,
                    key: "key".to_string(),
                },
                right: Literal::String("value".to_string()),
            }],
        );
        assert_eq!(res, expected);

        let res = parse_filter("key = 'value'").unwrap();
        let expected = (
            "",
            vec![Comparison {
                operator: Comparator::Equal,
                left: Identifier {
                    entity: Entity::Attribute,
                    key: "key".to_string(),
                },
                right: Literal::String("value".to_string()),
            }],
        );
        assert_eq!(res, expected);

        let res = parse_filter("attribute.key = 'value' AND metric.key > 0.5").unwrap();
        let expected = (
            "",
            vec![
                Comparison {
                    operator: Comparator::Equal,
                    left: Identifier {
                        entity: Entity::Attribute,
                        key: "key".to_string(),
                    },
                    right: Literal::String("value".to_string()),
                },
                Comparison {
                    operator: Comparator::GreaterThan,
                    left: Identifier {
                        entity: Entity::Metric,
                        key: "key".to_string(),
                    },
                    right: Literal::Float(0.5),
                },
            ],
        );
        assert_eq!(res, expected);
    }
}
