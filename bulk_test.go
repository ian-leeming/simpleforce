package simpleforce

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSalesforceTime(t *testing.T) {
	this := &struct {
		Time SalesforceTime `json:"time"`
	}{}
	err := json.Unmarshal([]byte(`{"time":"2023-12-02T02:30:02.000+0000"}`), this)

	if err != nil {
		t.Fatal(err)
	}
	if !this.Time.Equal(time.Date(2023, 12, 2, 2, 30, 2, 0, time.UTC)) {
		t.Fatal("parsed SalesforceTime does not equal expected time")
	}
}
