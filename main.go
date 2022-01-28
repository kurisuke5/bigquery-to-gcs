package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/joho/godotenv"
	"github.com/kurisuke5/BigQuery2GCS/pkg/config"
)

func main() {
	http.HandleFunc("/cron", handler)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		err := godotenv.Load(".env")
		if err != nil {
			log.Fatal("failed to godotenv load: %w", err)
		}
	}

	cfg, err := config.ReadConfig(ctx)
	if err != nil {
		log.Fatal("failed to read config: %w", err)
	}
	exportCSVToGCS(ctx, cfg)
}

// exportToGCS - BigQueryのテーブル指定してGCSにexport
func exportCSVToGCS(ctx context.Context, cfg *config.Config) {
	client, err := bigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		log.Fatal("failed to initialize bigquery client: %w", err)
	}

	uri := cfg.GCSURI + "YYYYMM.csv" // TODO ファイル名を修正
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.FieldDelimiter = ","
	gcsRef.AllowQuotedNewlines = true

	extractor := client.Dataset(cfg.DatasetID).Table(cfg.TableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = false

	job, err := extractor.Run(ctx)
	if err != nil {
		log.Fatal("failed to bigquery query run: %w", err)
	}
	jobStatus, err := job.Wait(ctx)
	if err != nil {
		log.Fatal("failed to bigquery job wait: %w", err)
	}
	if jobStatus.Err(); err != nil {
		log.Fatal("failed to bigquery job status: %w", err)
	}
}
