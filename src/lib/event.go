package lib

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

type LogEvent struct {
	LogID      string    `json:"log_id"`
	Timestamp  time.Time `json:"timestamp"`
	Method     string    `json:"method"`
	URI        string    `json:"uri"`
	Protocol   string    `json:"protocol"`
	StatusCode int32     `json:"status_code"`
	BytesSent  int64     `json:"bytes_sent"`
	UserAgent  string    `json:"user_agent"`
	ClientID   string    `json:"client_id"`
}

type ParquetRow struct {
	Timestamp  int64  `parquet:"timestamp,timestamp(millisecond)"`
	Method     string `parquet:"method,dict"`
	URI        string `parquet:"uri"`
	Protocol   string `parquet:"protocol,dict"`
	StatusCode int32  `parquet:"status_code"`
	BytesSent  int64  `parquet:"bytes_sent"`
	UserAgent  string `parquet:"user_agent,dict"`
	ClientID   string `parquet:"client_id,dict"`
}

var (
	methods = []string{
		"GET", "GET", "GET", "GET", "GET", "GET", "GET",
		"POST", "POST", "POST",
		"PUT",
		"DELETE",
		"HEAD",
		"OPTIONS",
	}

	protocols = []string{
		"HTTP/1.1", "HTTP/1.1", "HTTP/1.1", "HTTP/1.1", "HTTP/1.1",
		"HTTP/2.0", "HTTP/2.0", "HTTP/2.0",
		"HTTP/1.0",
	}

	statusCodes = []int32{
		200, 200, 200, 200, 200, 200, 200, 200,
		301, 302,
		304, 304, 304,
		400,
		404, 404,
		500,
	}

	uriTemplates = []string{
		"/",
		"/products",
		"/products/%d",
		"/products/%d/reviews",
		"/categories/%s",
		"/search?q=%s&page=%d",
		"/cart",
		"/cart/add",
		"/checkout",
		"/checkout/confirm",
		"/orders/%d",
		"/account/login",
		"/account/register",
		"/account/profile",
		"/api/v1/products",
		"/api/v1/products/%d",
		"/api/v1/search?q=%s",
		"/static/js/main.js",
		"/static/css/app.css",
		"/favicon.ico",
		"/robots.txt",
		"/health",
	}

	countryCodes = []string{
		"US", "US", "US", "US",
		"GB", "GB",
		"DE", "DE",
		"FR",
		"PL", "PL",
		"CA",
		"AU",
		"IN",
		"BR",
		"JP",
	}
)

func CreateLogEvent() LogEvent {
	return LogEvent{
		LogID:      gofakeit.UUID(),
		Timestamp:  time.Now().UTC(),
		Method:     methods[gofakeit.Number(0, len(methods)-1)],
		URI:        generateURI(),
		Protocol:   protocols[gofakeit.Number(0, len(protocols)-1)],
		StatusCode: statusCodes[gofakeit.Number(0, len(statusCodes)-1)],
		BytesSent:  generateBytesSent(),
		UserAgent:  gofakeit.UserAgent(),
		ClientID:   generateClientID(),
	}
}

func (e LogEvent) ToParquetRow() ParquetRow {
	return ParquetRow{
		Timestamp:  e.Timestamp.UnixMilli(),
		Method:     e.Method,
		URI:        e.URI,
		Protocol:   e.Protocol,
		StatusCode: e.StatusCode,
		BytesSent:  e.BytesSent,
		UserAgent:  e.UserAgent,
		ClientID:   e.ClientID,
	}
}

func ToParquetRows(events []LogEvent) []ParquetRow {
	rows := make([]ParquetRow, len(events))

	for index, event := range events {
		rows[index] = event.ToParquetRow()
	}

	return rows
}

func generateURI() string {
	tmpl := uriTemplates[gofakeit.Number(0, len(uriTemplates)-1)]
	switch tmpl {
	case "/products/%d", "/products/%d/reviews", "/orders/%d":
		return fmt.Sprintf(tmpl, gofakeit.Number(1, 99999))
	case "/categories/%s":
		cats := []string{"electronics", "clothing", "books", "home", "sports", "toys"}
		return fmt.Sprintf(tmpl, cats[gofakeit.Number(0, len(cats)-1)])
	case "/search?q=%s&page=%d":
		return fmt.Sprintf(tmpl, gofakeit.Word(), gofakeit.Number(1, 20))
	case "/api/v1/search?q=%s":
		return fmt.Sprintf(tmpl, gofakeit.Word())
	default:
		return tmpl
	}
}

func generateBytesSent() int64 {
	roll := gofakeit.Number(0, 99)
	switch {
	case roll < 40:
		return int64(gofakeit.Number(100, 2_000))
	case roll < 70:
		return int64(gofakeit.Number(2_000, 50_000))
	case roll < 90:
		return int64(gofakeit.Number(50_000, 500_000))
	default:
		return int64(gofakeit.Number(500_000, 5_000_000))
	}
}

func generateClientID() string {
	id := gofakeit.Number(1, 999999)
	country := countryCodes[gofakeit.Number(0, len(countryCodes)-1)]
	return fmt.Sprintf("%d%s", id, country)
}
