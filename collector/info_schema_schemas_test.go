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

package collector

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func TestScrapeSchemas(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery(sanitizeQuery(dbListQuery)).WillReturnRows(sqlmock.NewRows([]string{"schema_name"}).
		AddRow("mysql"))

	columns := []string{"TABLE_SCHEMA", "data_length", "index_length", "data_free", "schema_rows"}
	rows := sqlmock.NewRows(columns).
		AddRow("mysql", 40, 99, 1, 0)
	mock.ExpectQuery(sanitizeQuery(fmt.Sprintf(schemaDataQuery, "mysql"))).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = (ScrapeSchemas{}).Scrape(context.Background(), db, ch); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	expected := []MetricResult{
		{labels: labelMap{"schema": "mysql"}, value: 40, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"schema": "mysql"}, value: 99, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"schema": "mysql"}, value: 1, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"schema": "mysql"}, value: 0, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			got := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, got)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
