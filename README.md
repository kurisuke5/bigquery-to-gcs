# bigquery-to-gcs

1. `.env.template` ファイルを元に `.env` を作成
2. `.env` の中身を設定する
3. `go run main.go` を実行
4. curl 'http://localhost:8080/cron'
5. `.env` で指定したGCS_URIのバケットにcsvファイルが保存される
