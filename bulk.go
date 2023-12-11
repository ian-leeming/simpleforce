package simpleforce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type JobStateEnum string

const (
	UploadComplete JobStateEnum = "UploadComplete"
	InProgress     JobStateEnum = "InProgress"
	Aborted        JobStateEnum = "Aborted"
	JobComplete    JobStateEnum = "JobComplete"
	Failed         JobStateEnum = "Failed"
)

func (j JobStateEnum) IsFinished() bool {
	return j == JobComplete || j == Failed || j == Aborted
}

func (j JobStateEnum) ToError() error {
	switch j {
	case Aborted:
		return fmt.Errorf("job aborted")
	case Failed:
		return fmt.Errorf("job failed")
	}
	return nil
}

type BulkJobResultSet struct {
	Body *bytes.Buffer
	Next string
	Rows int
}

type BulkJobStatus struct {
	State                           JobStateEnum `json:"state"`
	NumberRecordsProcessed          int64        `json:"numberRecordsProcessed"`
	Retries                         int64        `json:"retries"`
	TotalProcessingTimeMilliseconds int64        `json:"totalProcessingTime"`
}

type SalesforceTime struct {
	time.Time
}

func (t *SalesforceTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" {
		return nil
	}
	// parsing time \"2023-12-02T02:30:02.000+0000\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"+0000\" as \"Z07:00\""
	tt, err := time.Parse(`"2006-01-02T15:04:05.000-0700"`, s)
	if err != nil {
		return err
	}
	*t = SalesforceTime{tt}
	return nil
}

type BulkJob struct {
	client          *Client
	Id              string         `json:"id"`
	Operation       string         `json:"operation"`
	Object          string         `json:"object"`
	CreatedById     string         `json:"createdById"`
	CreatedDate     SalesforceTime `json:"createdDate"`
	SystemModstamp  SalesforceTime `json:"systemModstamp"`
	State           string         `json:"state"`
	ConcurrencyMode string         `json:"concurrencyMode"`
	ContentType     string         `json:"contentType"`
	ApiVersion      float64        `json:"apiVersion"`
	LineEnding      string         `json:"lineEnding"`
	ColumnDelimiter string         `json:"columnDelimiter"`
}

func (job *BulkJob) GetStatus() (*BulkJobStatus, error) {
	url := job.client.makeURL(fmt.Sprintf("jobs/query/%s", job.Id))
	b, err := job.client.httpRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	status := &BulkJobStatus{}
	if err := json.Unmarshal(b, status); err != nil {
		sfErr := ParseSalesforceError(0, b)
		return nil, errors.Join(sfErr, err)
	}
	return status, nil
}

func (job *BulkJob) Wait(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		status, err := job.GetStatus()
		if err != nil {
			return err
		}
		if status.State.IsFinished() {
			return status.State.ToError()
		}
		time.Sleep(10 * time.Second)
	}
}

func (job *BulkJob) GetResultSet(locator string) (*BulkJobResultSet, error) {
	url := job.client.makeURL(fmt.Sprintf("jobs/query/%s/results", job.Id))
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "text/csv")
	req.Header.Add("Authorization", "Bearer "+job.client.sessionID)

	resp, err := job.client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get result set from bulk job: %s body(%s)", resp.Status, string(b))
	}

	out := &bytes.Buffer{}
	if resp.ContentLength > 0 {
		out = bytes.NewBuffer(make([]byte, 0, resp.ContentLength))
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return nil, err
	}

	// rows will simply be 0 if the header can't be parsed
	rows, _ := strconv.Atoi(
		resp.Header.Get("Sforce-NumberOfRecords"),
	)
	return &BulkJobResultSet{
		Body: out,
		Next: resp.Header.Get("Sforce-Locator"),
		Rows: rows,
	}, nil
}

func (job *BulkJob) Delete() error {
	url := job.client.makeURL(fmt.Sprintf("jobs/query/%s", job.Id))
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+job.client.sessionID)

	resp, err := job.client.httpClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete bulk job: %s body(%s)", resp.Status, string(b))
	}
	return nil
}
