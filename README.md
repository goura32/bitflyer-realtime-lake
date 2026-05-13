# bitflyer-realtime-lake

bitFlyer Realtime API の public channel を取得し、いったん SQLite spool に退避してから ClickHouse へ raw JSON として保存する Python ツールです。collector と writer を 1:1 の非同期タスクとして同一プロセスで動かし、ClickHouse 停止時にも受信データを落としにくい構成にしています。

## ツール概要

- public channel のみを対象にした realtime collector
- 受信直後は ClickHouse へ直接書かず、SQLite spool へ永続化
- writer が未送信データを batch insert
- raw_events と種別別テーブルへ二重保存
- canonical JSON + `sha256(channel + "\n" + canonical_json(payload))` で重複排除

## 対象データ

- `lightning_executions_{product_code}`
- `lightning_ticker_{product_code}`
- `lightning_board_snapshot_{product_code}`
- `lightning_board_{product_code}`

private channel は対象外です。

## アーキテクチャ

1. collector task
   bitFlyer Realtime API へ WebSocket 接続し、指定 channel を subscribe します。
2. spool
   受信 payload を SQLite WAL spool に保存します。
3. writer task
   未送信行を読み出して ClickHouse へ batch insert し、成功後に `shipped_at` を更新します。

同じイベントは以下の 2 系統へ保存されます。

- `raw_events`
- 種別別テーブル
  `raw_executions` / `raw_tickers` / `raw_board_snapshots` / `raw_board_deltas`

## セットアップ方法

Python 3.11 以上を使ってください。

```bash
git clone https://github.com/goura32/bitflyer-realtime-lake.git
cd bitflyer-realtime-lake
cp config.example.yaml config.yaml
cp .env.example .env
uv venv --python 3.11
source .venv/bin/activate
uv pip install -e ".[dev]"
```

`pip` でもインストールできます。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## config.yaml の作り方

`config.example.yaml` をコピーして `config.yaml` を作成します。

```yaml
bitflyer:
  endpoint: "wss://ws.lightstream.bitflyer.com/json-rpc"
  product_codes:
    - "BTC_JPY"
    - "FX_BTC_JPY"
    - "ETH_BTC"
  channel_types:
    - "executions"
    - "ticker"
    - "board_snapshot"
    - "board"

collector:
  instance_id: "auto"
  reconnect_initial_seconds: 1
  reconnect_max_seconds: 30
  receive_timeout_seconds: 60

spool:
  path: "./spool/events.sqlite3"
  sqlite_synchronous: "NORMAL"

clickhouse:
  host: "server.local"
  port: 8123
  database: "clickhouse"
  username: "clickhouse"
  password: ""
  batch_size: 1000
  flush_interval_seconds: 1.0
  retry_interval_seconds: 5.0
```

## .env の使い方

ClickHouse 接続情報は `config.yaml` にも書けますが、public repository に実パスワードを置かないため `.env` を優先利用できます。以下の環境変数をサポートしています。

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_USERNAME`
- `CLICKHOUSE_PASSWORD`

`.env` は `.gitignore` に含めています。

## ClickHouse 初期化方法

```bash
python -m bitflyer_realtime_lake init-db --config config.yaml
```

`sql/001_create_tables.sql` を使って DB とテーブルを作成します。既定 database 名は `clickhouse` ですが、config で変更できます。

## 起動方法

通常運用:

```bash
python -m bitflyer_realtime_lake run --config config.yaml
```

collector のみ:

```bash
python -m bitflyer_realtime_lake collect --config config.yaml
```

writer のみ:

```bash
python -m bitflyer_realtime_lake ship --config config.yaml
```

spool 状態確認:

```bash
python -m bitflyer_realtime_lake status --config config.yaml
```

## 複数起動する場合の workdir 分離

複数起動する場合は、各プロセスごとに別 workdir を使ってください。

```text
runs/run-a/
  config.yaml
  spool/events.sqlite3
  logs/

runs/run-b/
  config.yaml
  spool/events.sqlite3
  logs/
```

`--workdir` を指定すると、その workdir 基準で `./spool/events.sqlite3` を解決します。

```bash
python -m bitflyer_realtime_lake run --workdir runs/run-a --config runs/run-a/config.yaml
python -m bitflyer_realtime_lake run --workdir runs/run-b --config runs/run-b/config.yaml
```

## SQLite spool の役割

- ClickHouse が一時停止しても受信データを先にローカル保存できます
- writer は復旧後に未送信データを順次送れます
- SQLite は WAL モードで起動します
- 起動時 PRAGMA:
  `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`

Raspberry Pi では、spool 保存先を microSD ではなく SSD に置くことを推奨します。

## 重複排除の考え方

重複排除は raw データ完全一致時だけ行います。

1. payload を JSON として parse
2. キー順ソート済み canonical JSON を作成
3. `sha256(channel + "\n" + canonical_json(payload))` を計算

`received_at` や `collector_instance_id` は hash に含めません。これにより、複数 collector が同じ payload を受信しても同一イベントとして扱えます。

SQLite spool では `payload_hash` に unique index を持たせ、同一 spool 内の完全一致重複を `INSERT OR IGNORE` で抑止します。

## ReplacingMergeTree の注意

ClickHouse 側の重複排除は `ReplacingMergeTree` を使いますが、重複が即時に消える保証はありません。マージタイミングまでは同じ `payload_hash` を持つ行が見えることがあります。

## 板情報について

- `lightning_board_snapshot_*` は `raw_board_snapshots`
- `lightning_board_*` は `raw_board_deltas`

いずれも `payload_json` に raw のまま保存します。`bids` / `asks` の展開や、snapshot と delta を組み合わせた板再構築は今回の実装には含めていません。これは今後の検討対象です。

## トラブルシュート

- `status` で `unshipped rows` が増え続ける
  ClickHouse 接続情報、到達性、認証情報を確認してください。
- `failed rows` が増える
  writer が再試行中です。`last_error` を SQLite で確認してください。
- WebSocket が切れる
  collector は 1 秒から最大 30 秒の backoff で自動再接続し、再接続後に全 channel を再 subscribe します。
- Raspberry Pi で遅い
  spool を SSD へ置き、`batch_size` と `flush_interval_seconds` を調整してください。

## テスト

```bash
python -m pytest
```

ClickHouse 実 DB が必要なテストは `integration` marker に分ける想定です。既定では unit test のみで動作確認できます。
