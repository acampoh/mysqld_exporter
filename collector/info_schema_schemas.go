// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `information_schema.tables`.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	schemaDataQuery = `
	SELECT 
		TABLE_SCHEMA,
		SUM(DATA_LENGTH) as data_length,
		SUM(INDEX_LENGTH) as index_length,
		SUM(DATA_FREE) as data_free,
		SUM(TABLE_ROWS) as schema_rows
	  FROM information_schema.TABLES 
	  WHERE TABLE_SCHEMA = '%s'
	  GROUP BY TABLE_SCHEMA
	`
)

// Tunable flags.
var ()

// Metric descriptors.
var (
	infoSchemaSchemaDataLength = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "schema_data_length"),
		"The size of the data of the schema from information_schema.tables (table aggregation)",
		[]string{"schema"}, nil,
	)
	infoSchemaSchemaIndexLength = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "schema_index_length"),
		"The size of the indexes of the schema from information_schema.tables (table aggregation)",
		[]string{"schema"}, nil,
	)
	infoSchemaSchemaDataFree = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "schema_data_free"),
		"The size of the free data of the schema from information_schema.tables (table aggregation)",
		[]string{"schema"}, nil,
	)
	infoSchemaSchemaRows = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "schema_rows"),
		"The number of rows in the schema from information_schema.tables (table aggregation)",
		[]string{"schema"}, nil,
	)
)

// ScrapeSchemas collects from `information_schema.tables`.
type ScrapeSchemas struct{}

// Name of the Scraper. Should be unique.
func (ScrapeSchemas) Name() string {
	return informationSchema + ".schemas"
}

// Help describes the role of the Scraper.
func (ScrapeSchemas) Help() string {
	return "Collect schema stats from information_schema.tables"
}

// Version of MySQL from which scraper is available.
func (ScrapeSchemas) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeSchemas) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	var dbList []string
	if *tableSchemaDatabases == "*" {
		dbListRows, err := db.QueryContext(ctx, dbListQuery)
		if err != nil {
			return err
		}
		defer dbListRows.Close()

		var database string

		for dbListRows.Next() {
			if err := dbListRows.Scan(
				&database,
			); err != nil {
				return err
			}
			dbList = append(dbList, database)
		}
	} else {
		dbList = strings.Split(*tableSchemaDatabases, ",")
	}

	for _, database := range dbList {
		tableSchemaRows, err := db.QueryContext(ctx, fmt.Sprintf(schemaDataQuery, database))
		if err != nil {
			return err
		}
		defer tableSchemaRows.Close()

		var (
			tableSchema string
			dataLength  uint64
			indexLength uint64
			freeData    uint64
			schemaRows  uint64
		)

		for tableSchemaRows.Next() {
			err = tableSchemaRows.Scan(
				&tableSchema,
				&dataLength,
				&indexLength,
				&freeData,
				&schemaRows,
			)
			if err != nil {
				return err
			}
			ch <- prometheus.MustNewConstMetric(
				infoSchemaSchemaDataLength, prometheus.GaugeValue, float64(dataLength),
				tableSchema,
			)
			ch <- prometheus.MustNewConstMetric(
				infoSchemaSchemaIndexLength, prometheus.GaugeValue, float64(indexLength),
				tableSchema,
			)
			ch <- prometheus.MustNewConstMetric(
				infoSchemaSchemaDataFree, prometheus.GaugeValue, float64(freeData),
				tableSchema,
			)
			ch <- prometheus.MustNewConstMetric(
				infoSchemaSchemaRows, prometheus.GaugeValue, float64(schemaRows),
				tableSchema,
			)
		}
	}

	return nil
}

// check interface
var _ Scraper = ScrapeSchemas{}
