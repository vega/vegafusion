/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use crate::error::{Result, VegaFusionError};
use crate::expression::lexer::Token;

use crate::proto::gen::expression::{BinaryOperator, LogicalOperator, UnaryOperator};

pub fn unary_op_from_token(tok: &Token) -> Result<UnaryOperator> {
    Ok(match tok {
        Token::Plus => UnaryOperator::Pos,
        Token::Minus => UnaryOperator::Neg,
        Token::Exclamation => UnaryOperator::Not,
        t => {
            return Err(VegaFusionError::parse(&format!(
                "Token '{}' is not a valid prefix operator",
                t
            )))
        }
    })
}

pub fn binary_op_from_token(value: &Token) -> Result<BinaryOperator> {
    Ok(match value {
        Token::Plus => BinaryOperator::Plus,
        Token::Minus => BinaryOperator::Minus,
        Token::Asterisk => BinaryOperator::Mult,
        Token::Slash => BinaryOperator::Div,
        Token::Percent => BinaryOperator::Mod,
        Token::DoubleEquals => BinaryOperator::Equals,
        Token::TripleEquals => BinaryOperator::StrictEquals,
        Token::ExclamationEquals => BinaryOperator::NotEquals,
        Token::ExclamationDoubleEquals => BinaryOperator::NotStrictEquals,
        Token::GreaterThan => BinaryOperator::GreaterThan,
        Token::GreaterThanEquals => BinaryOperator::GreaterThanEqual,
        Token::LessThan => BinaryOperator::LessThan,
        Token::LessThanEquals => BinaryOperator::LessThanEqual,
        t => {
            return Err(VegaFusionError::parse(&format!(
                "Token '{}' is not a valid binary operator",
                t
            )))
        }
    })
}

pub fn logical_op_from_token(value: &Token) -> Result<LogicalOperator> {
    Ok(match value {
        Token::LogicalOr => LogicalOperator::Or,
        Token::LogicalAnd => LogicalOperator::And,
        t => {
            return Err(VegaFusionError::parse(&format!(
                "Token '{}' is not a valid logical operator",
                t
            )))
        }
    })
}
