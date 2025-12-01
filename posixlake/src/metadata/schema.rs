//! Schema definition and versioning using Arrow with nom parsers

use crate::Result;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, digit1, multispace0},
    combinator::{map, opt, recognize},
    multi::separated_list0,
    sequence::{delimited, preceded},
    IResult, Parser,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Database schema with versioning
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub version: u64,
    pub fields: Vec<SchemaField>,
}

/// Schema field definition with complex type support
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaField {
    pub name: String,
    pub data_type: DataTypeRepr,
    pub nullable: bool,
}

/// Serializable representation of Arrow DataType
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataTypeRepr {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp(TimeUnitRepr, Option<String>),
    Time32(TimeUnitRepr),
    Time64(TimeUnitRepr),
    Duration(TimeUnitRepr),
    Interval(IntervalUnitRepr),
    List(Box<SchemaField>),
    LargeList(Box<SchemaField>),
    FixedSizeList(Box<SchemaField>, i32),
    Struct(Vec<SchemaField>),
    Map(Box<SchemaField>, bool),
    Decimal128(u8, i8),
    Decimal256(u8, i8),
    Dictionary(Box<DataTypeRepr>, Box<DataTypeRepr>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimeUnitRepr {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IntervalUnitRepr {
    YearMonth,
    DayTime,
    MonthDayNano,
}

/// Schema version tracker
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaVersion {
    pub version: u64,
    pub schema: Schema,
}

impl DataTypeRepr {
    /// Convert to Arrow DataType
    pub fn to_arrow(&self) -> DataType {
        match self {
            DataTypeRepr::Null => DataType::Null,
            DataTypeRepr::Boolean => DataType::Boolean,
            DataTypeRepr::Int8 => DataType::Int8,
            DataTypeRepr::Int16 => DataType::Int16,
            DataTypeRepr::Int32 => DataType::Int32,
            DataTypeRepr::Int64 => DataType::Int64,
            DataTypeRepr::UInt8 => DataType::UInt8,
            DataTypeRepr::UInt16 => DataType::UInt16,
            DataTypeRepr::UInt32 => DataType::UInt32,
            DataTypeRepr::UInt64 => DataType::UInt64,
            DataTypeRepr::Float16 => DataType::Float16,
            DataTypeRepr::Float32 => DataType::Float32,
            DataTypeRepr::Float64 => DataType::Float64,
            DataTypeRepr::Utf8 => DataType::Utf8,
            DataTypeRepr::LargeUtf8 => DataType::LargeUtf8,
            DataTypeRepr::Binary => DataType::Binary,
            DataTypeRepr::LargeBinary => DataType::LargeBinary,
            DataTypeRepr::Date32 => DataType::Date32,
            DataTypeRepr::Date64 => DataType::Date64,
            DataTypeRepr::Timestamp(unit, tz) => {
                DataType::Timestamp(unit.to_arrow(), tz.as_ref().map(|s| Arc::from(s.as_str())))
            }
            DataTypeRepr::Time32(unit) => DataType::Time32(unit.to_arrow()),
            DataTypeRepr::Time64(unit) => DataType::Time64(unit.to_arrow()),
            DataTypeRepr::Duration(unit) => DataType::Duration(unit.to_arrow()),
            DataTypeRepr::Interval(unit) => DataType::Interval(unit.to_arrow()),
            DataTypeRepr::List(field) => DataType::List(Arc::new(field.to_arrow())),
            DataTypeRepr::LargeList(field) => DataType::LargeList(Arc::new(field.to_arrow())),
            DataTypeRepr::FixedSizeList(field, size) => {
                DataType::FixedSizeList(Arc::new(field.to_arrow()), *size)
            }
            DataTypeRepr::Struct(fields) => {
                let arrow_fields: Vec<Field> = fields.iter().map(|f| f.to_arrow()).collect();
                DataType::Struct(arrow_fields.into())
            }
            DataTypeRepr::Map(field, sorted) => DataType::Map(Arc::new(field.to_arrow()), *sorted),
            DataTypeRepr::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
            DataTypeRepr::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
            DataTypeRepr::Dictionary(key, value) => {
                DataType::Dictionary(Box::new(key.to_arrow()), Box::new(value.to_arrow()))
            }
        }
    }

    /// Convert from Arrow DataType
    pub fn from_arrow(dt: &DataType) -> Self {
        match dt {
            DataType::Null => DataTypeRepr::Null,
            DataType::Boolean => DataTypeRepr::Boolean,
            DataType::Int8 => DataTypeRepr::Int8,
            DataType::Int16 => DataTypeRepr::Int16,
            DataType::Int32 => DataTypeRepr::Int32,
            DataType::Int64 => DataTypeRepr::Int64,
            DataType::UInt8 => DataTypeRepr::UInt8,
            DataType::UInt16 => DataTypeRepr::UInt16,
            DataType::UInt32 => DataTypeRepr::UInt32,
            DataType::UInt64 => DataTypeRepr::UInt64,
            DataType::Float16 => DataTypeRepr::Float16,
            DataType::Float32 => DataTypeRepr::Float32,
            DataType::Float64 => DataTypeRepr::Float64,
            DataType::Utf8 => DataTypeRepr::Utf8,
            DataType::LargeUtf8 => DataTypeRepr::LargeUtf8,
            DataType::Binary => DataTypeRepr::Binary,
            DataType::LargeBinary => DataTypeRepr::LargeBinary,
            DataType::Date32 => DataTypeRepr::Date32,
            DataType::Date64 => DataTypeRepr::Date64,
            DataType::Timestamp(unit, tz) => DataTypeRepr::Timestamp(
                TimeUnitRepr::from_arrow(unit),
                tz.as_ref().map(|s| s.to_string()),
            ),
            DataType::Time32(unit) => DataTypeRepr::Time32(TimeUnitRepr::from_arrow(unit)),
            DataType::Time64(unit) => DataTypeRepr::Time64(TimeUnitRepr::from_arrow(unit)),
            DataType::Duration(unit) => DataTypeRepr::Duration(TimeUnitRepr::from_arrow(unit)),
            DataType::Interval(unit) => DataTypeRepr::Interval(IntervalUnitRepr::from_arrow(unit)),
            DataType::List(field) => {
                DataTypeRepr::List(Box::new(SchemaField::from_arrow(field.as_ref())))
            }
            DataType::LargeList(field) => {
                DataTypeRepr::LargeList(Box::new(SchemaField::from_arrow(field.as_ref())))
            }
            DataType::FixedSizeList(field, size) => DataTypeRepr::FixedSizeList(
                Box::new(SchemaField::from_arrow(field.as_ref())),
                *size,
            ),
            DataType::Struct(fields) => {
                let schema_fields: Vec<SchemaField> = fields
                    .iter()
                    .map(|f| SchemaField::from_arrow(f.as_ref()))
                    .collect();
                DataTypeRepr::Struct(schema_fields)
            }
            DataType::Map(field, sorted) => {
                DataTypeRepr::Map(Box::new(SchemaField::from_arrow(field.as_ref())), *sorted)
            }
            DataType::Decimal128(precision, scale) => DataTypeRepr::Decimal128(*precision, *scale),
            DataType::Decimal256(precision, scale) => DataTypeRepr::Decimal256(*precision, *scale),
            DataType::Dictionary(key, value) => DataTypeRepr::Dictionary(
                Box::new(DataTypeRepr::from_arrow(key.as_ref())),
                Box::new(DataTypeRepr::from_arrow(value.as_ref())),
            ),
            _ => DataTypeRepr::Utf8, // Default for unsupported types
        }
    }

    /// Parse DataType from string representation using nom
    pub fn parse(input: &str) -> IResult<&str, DataTypeRepr> {
        alt((
            parse_simple_type,
            parse_timestamp,
            parse_time32,
            parse_time64,
            parse_duration,
            parse_interval,
            parse_decimal128,
            parse_decimal256,
            parse_list,
            parse_large_list,
            parse_fixed_size_list,
            parse_struct,
            parse_map,
            parse_dictionary,
        ))
        .parse(input)
    }
}

impl TimeUnitRepr {
    fn to_arrow(&self) -> TimeUnit {
        match self {
            TimeUnitRepr::Second => TimeUnit::Second,
            TimeUnitRepr::Millisecond => TimeUnit::Millisecond,
            TimeUnitRepr::Microsecond => TimeUnit::Microsecond,
            TimeUnitRepr::Nanosecond => TimeUnit::Nanosecond,
        }
    }

    fn from_arrow(unit: &TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => TimeUnitRepr::Second,
            TimeUnit::Millisecond => TimeUnitRepr::Millisecond,
            TimeUnit::Microsecond => TimeUnitRepr::Microsecond,
            TimeUnit::Nanosecond => TimeUnitRepr::Nanosecond,
        }
    }
}

impl IntervalUnitRepr {
    fn to_arrow(&self) -> arrow::datatypes::IntervalUnit {
        match self {
            IntervalUnitRepr::YearMonth => arrow::datatypes::IntervalUnit::YearMonth,
            IntervalUnitRepr::DayTime => arrow::datatypes::IntervalUnit::DayTime,
            IntervalUnitRepr::MonthDayNano => arrow::datatypes::IntervalUnit::MonthDayNano,
        }
    }

    fn from_arrow(unit: &arrow::datatypes::IntervalUnit) -> Self {
        match unit {
            arrow::datatypes::IntervalUnit::YearMonth => IntervalUnitRepr::YearMonth,
            arrow::datatypes::IntervalUnit::DayTime => IntervalUnitRepr::DayTime,
            arrow::datatypes::IntervalUnit::MonthDayNano => IntervalUnitRepr::MonthDayNano,
        }
    }
}

impl SchemaField {
    pub fn to_arrow(&self) -> Field {
        Field::new(&self.name, self.data_type.to_arrow(), self.nullable)
    }

    pub fn from_arrow(field: &Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: DataTypeRepr::from_arrow(field.data_type()),
            nullable: field.is_nullable(),
        }
    }
}

// Nom parser implementations

fn ws<'a, O, F>(inner: F) -> impl Parser<&'a str, Output = O, Error = nom::error::Error<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = nom::error::Error<&'a str>>,
{
    delimited(multispace0, inner, multispace0)
}

fn parse_simple_type(input: &str) -> IResult<&str, DataTypeRepr> {
    alt((
        map(tag("Null"), |_| DataTypeRepr::Null),
        map(tag("Boolean"), |_| DataTypeRepr::Boolean),
        map(tag("Int8"), |_| DataTypeRepr::Int8),
        map(tag("Int16"), |_| DataTypeRepr::Int16),
        map(tag("Int32"), |_| DataTypeRepr::Int32),
        map(tag("Int64"), |_| DataTypeRepr::Int64),
        map(tag("UInt8"), |_| DataTypeRepr::UInt8),
        map(tag("UInt16"), |_| DataTypeRepr::UInt16),
        map(tag("UInt32"), |_| DataTypeRepr::UInt32),
        map(tag("UInt64"), |_| DataTypeRepr::UInt64),
        map(tag("Float16"), |_| DataTypeRepr::Float16),
        map(tag("Float32"), |_| DataTypeRepr::Float32),
        map(tag("Float64"), |_| DataTypeRepr::Float64),
        map(tag("LargeUtf8"), |_| DataTypeRepr::LargeUtf8),
        map(tag("Utf8"), |_| DataTypeRepr::Utf8),
        map(tag("LargeBinary"), |_| DataTypeRepr::LargeBinary),
        map(tag("Binary"), |_| DataTypeRepr::Binary),
        map(tag("Date32"), |_| DataTypeRepr::Date32),
        map(tag("Date64"), |_| DataTypeRepr::Date64),
    ))
    .parse(input)
}

fn parse_time_unit(input: &str) -> IResult<&str, TimeUnitRepr> {
    alt((
        map(tag("Second"), |_| TimeUnitRepr::Second),
        map(tag("Millisecond"), |_| TimeUnitRepr::Millisecond),
        map(tag("Microsecond"), |_| TimeUnitRepr::Microsecond),
        map(tag("Nanosecond"), |_| TimeUnitRepr::Nanosecond),
    ))
    .parse(input)
}

fn parse_interval_unit(input: &str) -> IResult<&str, IntervalUnitRepr> {
    alt((
        map(tag("YearMonth"), |_| IntervalUnitRepr::YearMonth),
        map(tag("DayTime"), |_| IntervalUnitRepr::DayTime),
        map(tag("MonthDayNano"), |_| IntervalUnitRepr::MonthDayNano),
    ))
    .parse(input)
}

fn parse_timestamp(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Timestamp"),
            ws(char('(')),
            parse_time_unit,
            opt(preceded(ws(char(',')), parse_timezone)),
            ws(char(')')),
        ),
        |(_, _, unit, tz, _)| DataTypeRepr::Timestamp(unit, tz),
    )
    .parse(input)
}

fn parse_timezone(input: &str) -> IResult<&str, String> {
    map(
        delimited(char('"'), take_while1(|c| c != '"'), char('"')),
        |s: &str| s.to_string(),
    )
    .parse(input)
}

fn parse_time32(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (tag("Time32"), ws(char('(')), parse_time_unit, ws(char(')'))),
        |(_, _, unit, _)| DataTypeRepr::Time32(unit),
    )
    .parse(input)
}

fn parse_time64(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (tag("Time64"), ws(char('(')), parse_time_unit, ws(char(')'))),
        |(_, _, unit, _)| DataTypeRepr::Time64(unit),
    )
    .parse(input)
}

fn parse_duration(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Duration"),
            ws(char('(')),
            parse_time_unit,
            ws(char(')')),
        ),
        |(_, _, unit, _)| DataTypeRepr::Duration(unit),
    )
    .parse(input)
}

fn parse_interval(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Interval"),
            ws(char('(')),
            parse_interval_unit,
            ws(char(')')),
        ),
        |(_, _, unit, _)| DataTypeRepr::Interval(unit),
    )
    .parse(input)
}

fn parse_decimal128(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Decimal128"),
            ws(char('(')),
            ws(digit1),
            ws(char(',')),
            ws(recognize((opt(char('-')), digit1))),
            ws(char(')')),
        ),
        |(_, _, p, _, s, _)| {
            DataTypeRepr::Decimal128(p.parse().unwrap_or(38), s.parse().unwrap_or(0))
        },
    )
    .parse(input)
}

fn parse_decimal256(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Decimal256"),
            ws(char('(')),
            ws(digit1),
            ws(char(',')),
            ws(recognize((opt(char('-')), digit1))),
            ws(char(')')),
        ),
        |(_, _, p, _, s, _)| {
            DataTypeRepr::Decimal256(p.parse().unwrap_or(76), s.parse().unwrap_or(0))
        },
    )
    .parse(input)
}

fn parse_field_name(input: &str) -> IResult<&str, String> {
    map(
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        |s: &str| s.to_string(),
    )
    .parse(input)
}

fn parse_field(input: &str) -> IResult<&str, SchemaField> {
    map(
        (
            parse_field_name,
            ws(char(':')),
            DataTypeRepr::parse,
            opt(preceded(ws(char(',')), tag("nullable"))),
        ),
        |(name, _, data_type, nullable)| SchemaField {
            name,
            data_type,
            nullable: nullable.is_some(),
        },
    )
    .parse(input)
}

fn parse_list(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (tag("List"), ws(char('(')), parse_field, ws(char(')'))),
        |(_, _, field, _)| DataTypeRepr::List(Box::new(field)),
    )
    .parse(input)
}

fn parse_large_list(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (tag("LargeList"), ws(char('(')), parse_field, ws(char(')'))),
        |(_, _, field, _)| DataTypeRepr::LargeList(Box::new(field)),
    )
    .parse(input)
}

fn parse_fixed_size_list(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("FixedSizeList"),
            ws(char('(')),
            parse_field,
            ws(char(',')),
            ws(digit1),
            ws(char(')')),
        ),
        |(_, _, field, _, size, _)| {
            DataTypeRepr::FixedSizeList(Box::new(field), size.parse().unwrap_or(1))
        },
    )
    .parse(input)
}

fn parse_struct(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Struct"),
            ws(char('(')),
            separated_list0(ws(char(',')), parse_field),
            ws(char(')')),
        ),
        |(_, _, fields, _)| DataTypeRepr::Struct(fields),
    )
    .parse(input)
}

fn parse_map(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Map"),
            ws(char('(')),
            parse_field,
            ws(char(',')),
            alt((
                map(tag("sorted"), |_| true),
                map(tag("unsorted"), |_| false),
            )),
            ws(char(')')),
        ),
        |(_, _, field, _, sorted, _)| DataTypeRepr::Map(Box::new(field), sorted),
    )
    .parse(input)
}

fn parse_dictionary(input: &str) -> IResult<&str, DataTypeRepr> {
    map(
        (
            tag("Dictionary"),
            ws(char('(')),
            DataTypeRepr::parse,
            ws(char(',')),
            DataTypeRepr::parse,
            ws(char(')')),
        ),
        |(_, _, key, _, value, _)| DataTypeRepr::Dictionary(Box::new(key), Box::new(value)),
    )
    .parse(input)
}

impl Schema {
    pub fn new(fields: Vec<SchemaField>) -> Self {
        Self { version: 1, fields }
    }

    /// Convert Arrow schema to our Schema type
    pub fn from_arrow(arrow_schema: &ArrowSchema) -> Self {
        let fields = arrow_schema
            .fields()
            .iter()
            .map(|field| SchemaField::from_arrow(field.as_ref()))
            .collect();

        Self { version: 1, fields }
    }

    pub fn to_arrow(&self) -> Arc<ArrowSchema> {
        let fields: Vec<Field> = self.fields.iter().map(|f| f.to_arrow()).collect();
        Arc::new(ArrowSchema::new(fields))
    }

    /// Evolve the schema by adding a new field
    pub fn add_field(&mut self, field: SchemaField) {
        self.fields.push(field);
        self.version += 1;
    }

    /// Find a field by name
    pub fn find_field(&self, name: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.name == name)
    }
}

/// Schema manager for reading/writing schema.json
pub struct SchemaManager {
    metadata_dir: PathBuf,
}

impl SchemaManager {
    pub fn new<P: AsRef<Path>>(metadata_dir: P) -> Result<Self> {
        use std::fs;
        use tracing::info;

        let metadata_dir = metadata_dir.as_ref().to_path_buf();
        fs::create_dir_all(&metadata_dir)?;
        info!("Schema manager initialized at {}", metadata_dir.display());

        Ok(Self { metadata_dir })
    }

    pub fn write_schema(&self, schema: &Schema) -> Result<()> {
        use std::fs;
        use tracing::{debug, info};

        let schema_path = self.metadata_dir.join("schema.json");
        let tmp_path = self.metadata_dir.join("schema.json.tmp");

        debug!(
            "Writing schema version {} to {}",
            schema.version,
            schema_path.display()
        );

        let json = serde_json::to_string_pretty(schema)?;
        fs::write(&tmp_path, &json)?;
        fs::rename(&tmp_path, &schema_path)?;

        let history_path = self.metadata_dir.join("schema_history.json");
        let schema_version = SchemaVersion {
            version: schema.version,
            schema: schema.clone(),
        };

        let mut history = if history_path.exists() {
            let content = fs::read_to_string(&history_path)?;
            serde_json::from_str::<Vec<SchemaVersion>>(&content)?
        } else {
            Vec::new()
        };

        if !history.iter().any(|v| v.version == schema.version) {
            history.push(schema_version);
            let history_tmp = self.metadata_dir.join("schema_history.json.tmp");
            let history_json = serde_json::to_string_pretty(&history)?;
            fs::write(&history_tmp, &history_json)?;
            fs::rename(&history_tmp, &history_path)?;
        }

        info!("Successfully wrote schema version {}", schema.version);
        Ok(())
    }

    pub fn read_schema(&self) -> Result<Option<Schema>> {
        use std::fs;
        use tracing::debug;

        let schema_path = self.metadata_dir.join("schema.json");

        if !schema_path.exists() {
            return Ok(None);
        }

        debug!("Reading schema from {}", schema_path.display());

        let content = fs::read_to_string(&schema_path)?;
        let schema: Schema = serde_json::from_str(&content)?;

        Ok(Some(schema))
    }

    pub fn read_history(&self) -> Result<Vec<SchemaVersion>> {
        use std::fs;
        use tracing::debug;

        let history_path = self.metadata_dir.join("schema_history.json");

        if !history_path.exists() {
            return Ok(Vec::new());
        }

        debug!("Reading schema history from {}", history_path.display());

        let content = fs::read_to_string(&history_path)?;
        let history: Vec<SchemaVersion> = serde_json::from_str(&content)?;

        Ok(history)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_simple_types() {
        assert_eq!(DataTypeRepr::parse("Int32").unwrap().1, DataTypeRepr::Int32);
        assert_eq!(DataTypeRepr::parse("Utf8").unwrap().1, DataTypeRepr::Utf8);
        assert_eq!(
            DataTypeRepr::parse("Boolean").unwrap().1,
            DataTypeRepr::Boolean
        );
    }

    #[test]
    fn test_parse_timestamp() {
        let result = DataTypeRepr::parse("Timestamp(Microsecond, \"UTC\")");
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        assert!(
            matches!(dt, DataTypeRepr::Timestamp(TimeUnitRepr::Microsecond, Some(tz)) if tz == "UTC")
        );
    }

    #[test]
    fn test_parse_list() {
        let result = DataTypeRepr::parse("List(item:Int32)");
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        if let DataTypeRepr::List(field) = dt {
            assert_eq!(field.name, "item");
            assert_eq!(field.data_type, DataTypeRepr::Int32);
        } else {
            panic!("Expected List type");
        }
    }

    #[test]
    fn test_parse_struct() {
        let result = DataTypeRepr::parse("Struct(id:Int64,name:Utf8)");
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        if let DataTypeRepr::Struct(fields) = dt {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "id");
            assert_eq!(fields[1].name, "name");
        } else {
            panic!("Expected Struct type");
        }
    }

    #[test]
    fn test_parse_decimal() {
        let result = DataTypeRepr::parse("Decimal128(38, 10)");
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        assert_eq!(dt, DataTypeRepr::Decimal128(38, 10));
    }

    #[test]
    fn test_arrow_roundtrip() {
        let original = DataTypeRepr::Int64;
        let arrow = original.to_arrow();
        let back = DataTypeRepr::from_arrow(&arrow);
        assert_eq!(original, back);
    }

    #[test]
    fn test_complex_arrow_roundtrip() {
        let original = DataTypeRepr::Struct(vec![
            SchemaField {
                name: "id".to_string(),
                data_type: DataTypeRepr::Int64,
                nullable: false,
            },
            SchemaField {
                name: "values".to_string(),
                data_type: DataTypeRepr::List(Box::new(SchemaField {
                    name: "item".to_string(),
                    data_type: DataTypeRepr::Float64,
                    nullable: true,
                })),
                nullable: true,
            },
        ]);
        let arrow = original.to_arrow();
        let back = DataTypeRepr::from_arrow(&arrow);
        assert_eq!(original, back);
    }

    #[test]
    fn test_schema_creation() {
        let fields = vec![SchemaField {
            name: "id".to_string(),
            data_type: DataTypeRepr::Int64,
            nullable: false,
        }];
        let schema = Schema::new(fields);
        assert_eq!(schema.version, 1);
    }

    #[test]
    fn test_schema_add_field() {
        let mut schema = Schema::new(vec![SchemaField {
            name: "id".to_string(),
            data_type: DataTypeRepr::Int64,
            nullable: false,
        }]);

        assert_eq!(schema.version, 1);
        assert_eq!(schema.fields.len(), 1);

        schema.add_field(SchemaField {
            name: "name".to_string(),
            data_type: DataTypeRepr::Utf8,
            nullable: true,
        });

        assert_eq!(schema.version, 2);
        assert_eq!(schema.fields.len(), 2);
        assert!(schema.find_field("name").is_some());
    }

    #[test]
    fn test_schema_manager_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_dir = temp_dir.path().join("_metadata");

        let manager = SchemaManager::new(&metadata_dir).unwrap();

        let test_schema = Schema::new(vec![
            SchemaField {
                name: "id".to_string(),
                data_type: DataTypeRepr::Int64,
                nullable: false,
            },
            SchemaField {
                name: "email".to_string(),
                data_type: DataTypeRepr::Utf8,
                nullable: false,
            },
        ]);

        manager.write_schema(&test_schema).unwrap();

        let read_schema = manager.read_schema().unwrap();
        assert!(read_schema.is_some());
        assert_eq!(read_schema.unwrap(), test_schema);
    }
}
