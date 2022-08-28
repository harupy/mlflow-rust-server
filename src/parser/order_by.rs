use crate::parser::common::{identifier, Identifier};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace0;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::tuple;
use nom::IResult;

#[derive(Debug, PartialEq)]
pub struct OrderBy {
    pub identifier: Identifier,
    pub ascending: OrderByDirection,
}

#[derive(Debug, PartialEq)]
pub enum OrderByDirection {
    Ascending,
    Descending,
}

impl std::fmt::Display for OrderByDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            OrderByDirection::Ascending => write!(f, "ASC"),
            OrderByDirection::Descending => write!(f, "DESC"),
        }
    }
}

pub fn order_by_direction(i: &str) -> IResult<&str, OrderByDirection> {
    map(
        alt((tag_no_case("asc"), tag_no_case("desc"))),
        |s: &str| match s.to_lowercase().as_str() {
            "asc" => OrderByDirection::Ascending,
            "desc" => OrderByDirection::Descending,
            _ => unreachable!(),
        },
    )(i)
}

pub fn parse_order_by(i: &str) -> IResult<&str, OrderBy> {
    map(
        tuple((identifier, multispace0, opt(order_by_direction))),
        |tup| OrderBy {
            identifier: tup.0,
            ascending: match tup.2 {
                Some(d) => d,
                None => OrderByDirection::Ascending,
            },
        },
    )(i)
}

#[cfg(test)]
mod tests {
    use super::{order_by_direction, parse_order_by, OrderBy, OrderByDirection};
    use crate::parser::common::{Entity, Identifier};

    #[test]
    fn test_order_by_direction() {
        let res = order_by_direction("ASC").unwrap();
        assert_eq!(res, ("", OrderByDirection::Ascending));

        let res = order_by_direction("DESC").unwrap();
        assert_eq!(res, ("", OrderByDirection::Descending));
    }

    #[test]
    fn test_parse_order_by() {
        let res = parse_order_by("attr ASC").unwrap();
        assert_eq!(
            res,
            (
                "",
                OrderBy {
                    identifier: Identifier {
                        entity: Entity::Attribute,
                        key: "attr".to_string()
                    },
                    ascending: OrderByDirection::Ascending
                }
            )
        );

        let res = parse_order_by("attr DESC").unwrap();
        assert_eq!(
            res,
            (
                "",
                OrderBy {
                    identifier: Identifier {
                        entity: Entity::Attribute,
                        key: "attr".to_string()
                    },
                    ascending: OrderByDirection::Descending
                }
            )
        );

        let res = parse_order_by("attr").unwrap();
        assert_eq!(
            res,
            (
                "",
                OrderBy {
                    identifier: Identifier {
                        entity: Entity::Attribute,
                        key: "attr".to_string()
                    },
                    ascending: OrderByDirection::Ascending
                }
            )
        );

        let res = parse_order_by("attribute.attr").unwrap();
        assert_eq!(
            res,
            (
                "",
                OrderBy {
                    identifier: Identifier {
                        entity: Entity::Attribute,
                        key: "attr".to_string()
                    },
                    ascending: OrderByDirection::Ascending
                }
            )
        );

        let res = parse_order_by("attr   ASC").unwrap();
        assert_eq!(
            res,
            (
                "",
                OrderBy {
                    identifier: Identifier {
                        entity: Entity::Attribute,
                        key: "attr".to_string()
                    },
                    ascending: OrderByDirection::Ascending
                }
            )
        );
    }
}
