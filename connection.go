package spanner

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/Mitu217/tamate/driver"
	"google.golang.org/api/iterator"
)

type spannerConn struct {
	DSN           string
	spannerClient *spanner.Client
}

func (c *spannerConn) Close() error {
	if c.spannerClient != nil {
		c.spannerClient.Close()
	}
	return nil
}

func (c *spannerConn) createAllSchemaMap(ctx context.Context) (map[string]*driver.Schema, error) {
	stmt := spanner.NewStatement("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''")
	iter := c.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	// scan results
	schemaMap := make(map[string]*driver.Schema)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var tableName string
		if err := row.ColumnByName("TABLE_NAME", &tableName); err != nil {
			return nil, err
		}

		column, err := ScanSchemaColumn(row)
		if err != nil {
			return nil, err
		}
		if _, ok := schemaMap[tableName]; !ok {
			schemaMap[tableName] = &driver.Schema{Name: tableName}
		}
		schemaMap[tableName].Columns = append(schemaMap[tableName].Columns, column)
	}

	for tableName, schema := range schemaMap {
		pk, err := c.getPrimaryKey(ctx, tableName)
		if err != nil {
			return nil, err
		}
		schema.PrimaryKey = pk
	}
	return schemaMap, nil
}

func ScanSchemaColumn(row *spanner.Row) (*driver.Column, error) {
	var columnName string
	var tableName string
	var ordinalPosition int64
	var columnType string
	var isNullable string
	if err := row.Columns(&tableName, &columnName, &ordinalPosition, &columnType, &isNullable); err != nil {
		return nil, err
	}
	ct, err := spannerTypeNameToColumnType(columnType)
	if err != nil {
		return nil, err
	}
	return &driver.Column{
		Name:            columnName,
		OrdinalPosition: int(ordinalPosition),
		Type:            ct,
		NotNull:         isNullable == "NO",
		AutoIncrement:   false, // Cloud Spanner does not support AUTO_INCREMENT
	}, nil
}

func spannerTypeNameToColumnType(st string) (driver.ColumnType, error) {

	if st == "INT64" {
		return driver.ColumnTypeInt, nil
	}
	if st == "FLOAT64" {
		return driver.ColumnTypeFloat, nil
	}
	if st == "TIMESTAMP" {
		return driver.ColumnTypeDatetime, nil
	}
	if st == "DATE" {
		return driver.ColumnTypeDate, nil
	}
	if st == "BOOL" {
		return driver.ColumnTypeBool, nil
	}
	if strings.HasPrefix(st, "STRING") {
		return driver.ColumnTypeString, nil
	}
	if strings.HasPrefix(st, "BYTES") {
		return driver.ColumnTypeBytes, nil
	}

	// This is a little suck, but for now it's just enough.
	if strings.HasPrefix(st, "ARRAY<STRING") {
		return driver.ColumnTypeStringArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<BYTES") {
		return driver.ColumnTypeBytesArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<DATE") {
		return driver.ColumnTypeDateArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<FLOAT64") {
		return driver.ColumnTypeFloatArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<INT64") {
		return driver.ColumnTypeIntArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<TIMESTAMP") {
		return driver.ColumnTypeDatetimeArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<BOOL") {
		return driver.ColumnTypeBoolArray, nil
	}

	return driver.ColumnTypeNull, fmt.Errorf("cannot convert spanner type: %s", st)
}

func (c *spannerConn) getPrimaryKey(ctx context.Context, tableName string) (*driver.Key, error) {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME = '%s' AND INDEX_TYPE = 'PRIMARY_KEY' ORDER BY ORDINAL_POSITION ASC", tableName))
	iter := c.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var pk *driver.Key
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if pk == nil {
			pk = &driver.Key{}
		}
		var colName string
		if err := row.ColumnByName("COLUMN_NAME", &colName); err != nil {
			return nil, err
		}
		pk.KeyType = driver.KeyTypePrimary
		pk.ColumnNames = append(pk.ColumnNames, colName)
	}
	return pk, nil
}

func (c *spannerConn) GetSchema(ctx context.Context, name string) (*driver.Schema, error) {
	all, err := c.createAllSchemaMap(ctx)
	if err != nil {
		return nil, err
	}

	for scName, sc := range all {
		if scName == name {
			return sc, nil
		}
	}
	return nil, errors.New("Schema not found: " + name)
}

func (c *spannerConn) SetSchema(ctx context.Context, name string, schema *driver.Schema) error {
	return errors.New("not implemented")
}

func (c *spannerConn) GetRows(ctx context.Context, name string) ([]*driver.Row, error) {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT * FROM `%s`", name))
	iter := c.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var rows []*driver.Row
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		/*
			rowValues := make(driver.RowValues)
			rowValuesGroupByKey := make(driver.GroupByKey)
			for _, c := range schema.Columns {
				var gval spanner.GenericColumnValue
				if err := row.ColumnByName(c.Name, &gval); err != nil {
					return nil, err
				}
				cv, err := GenericSpannerValueToTamateGenericColumnValue(gval, c)
				if err != nil {
					return nil, err
				}
				rowValues[c.Name] = cv
				for _, name := range schema.PrimaryKey.ColumnNames {
					if name == c.Name {
						rowValuesGroupByKey[schema.PrimaryKey.String()] = append(rowValuesGroupByKey[schema.PrimaryKey.String()], cv)
					}
				}
			}
			rows = append(rows, &driver.Row{rowValuesGroupByKey, rowValues})
		*/
	}
	return rows, nil
}

func (c *spannerConn) SetRows(ctx context.Context, name string, rows []*driver.Row) error {
	return errors.New("SpannerDatasource does not support SetRows()")
}
