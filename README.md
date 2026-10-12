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

## ローカル ClickHouse を docker-compose で起動する例

ローカル検証用に [docker-compose.clickhouse.yaml](/Users/goura32/Documents/Codex/2026-05-13/python-github-public-bitflyer-realtime-api/docker-compose.clickhouse.yaml) を用意しています。

起動:

```bash
docker compose -f docker-compose.clickhouse.yaml up -d
```

停止:

```bash
docker compose -f docker-compose.clickhouse.yaml down
```

初回起動後は `config.yaml` か `.env` を以下に合わせると、そのまま接続確認できます。

```env
CLICKHOUSE_HOST=127.0.0.1
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=clickhouse
CLICKHOUSE_USERNAME=clickhouse
CLICKHOUSE_PASSWORD=clickhouse
```

データは `./local/clickhouse/data`、ログは `./local/clickhouse/logs` に保存されます。

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

## systemd で運用する例

Ubuntu や Raspberry Pi OS で常駐化する場合は、[ops/systemd/bitflyer-realtime-lake.service.example](/Users/goura32/Documents/Codex/2026-05-13/python-github-public-bitflyer-realtime-api/ops/systemd/bitflyer-realtime-lake.service.example) をたたき台として使えます。

1. 配置先ディレクトリを決めます。例: `/opt/bitflyer-realtime-lake`
2. 仮想環境を作成し、`pip install -e .` または `uv pip install -e .` を実行します
3. `config.yaml` を配置し、必要なら `.env` を同じ workdir に置きます
4. example ファイル内の `User`、`Group`、`WorkingDirectory`、`ExecStart` を実環境へ合わせて編集します
5. `/etc/systemd/system/bitflyer-realtime-lake.service` として保存します
6. 以下を実行します

```bash
sudo systemctl daemon-reload
sudo systemctl enable bitflyer-realtime-lake
sudo systemctl start bitflyer-realtime-lake
sudo systemctl status bitflyer-realtime-lake
```

ログ確認:

```bash
journalctl -u bitflyer-realtime-lake -f
```

複数起動したい場合は、workdir ごとに別 service を作ってください。例:

- `bitflyer-realtime-lake-run-a.service`
- `bitflyer-realtime-lake-run-b.service`
- `bitflyer-realtime-lake-run-c.service`

それぞれ `--workdir` と `--config` を別ディレクトリへ向けます。

## launchd で運用する例

macOS で常駐化する場合は、[ops/launchd/com.goura32.bitflyer-realtime-lake.plist.example](/Users/goura32/Documents/Codex/2026-05-13/python-github-public-bitflyer-realtime-api/ops/launchd/com.goura32.bitflyer-realtime-lake.plist.example) をたたき台として使えます。

1. example 内の `/Users/your-user/...` を実際の絶対パスへ置き換えます
2. `logs/` ディレクトリを作成します
3. `~/Library/LaunchAgents/com.goura32.bitflyer-realtime-lake.plist` として保存します
4. 以下を実行します

```bash
launchctl unload ~/Library/LaunchAgents/com.goura32.bitflyer-realtime-lake.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.goura32.bitflyer-realtime-lake.plist
launchctl start com.goura32.bitflyer-realtime-lake
```

状態確認:

```bash
launchctl list | grep bitflyer-realtime-lake
tail -f logs/launchd.stdout.log logs/launchd.stderr.log
```

複数起動したい場合は、plist を複製して `Label`、`--workdir`、`--config`、ログパスをそれぞれ分けてください。

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
- `docker compose` で ClickHouse が起動しない
  `docker-compose.clickhouse.yaml` の volume 保存先権限、使用中ポート `8123` / `9000`、メモリ使用量を確認してください。
- `failed rows` が増える
  writer が再試行中です。`last_error` を SQLite で確認してください。
- WebSocket が切れる
  collector は 1 秒から最大 30 秒の backoff で自動再接続し、再接続後に全 channel を再 subscribe します。
- systemd で起動しない
  `WorkingDirectory`、`ExecStart`、仮想環境の Python パス、service 実行ユーザーの書き込み権限を確認してください。
- launchd で起動しない
  plist 内の絶対パス、`logs/` の存在、`launchctl list` の出力、`launchd.stderr.log` を確認してください。
- Raspberry Pi で遅い
  spool を SSD へ置き、`batch_size` と `flush_interval_seconds` を調整してください。

## テスト

```bash
python -m pytest
```

ClickHouse 実 DB が必要なテストは `integration` marker に分ける想定です。既定では unit test のみで動作確認できます。


## SQLite spool の容量管理と送信のバッチ化

writer は、最初の未送信行を見つけてから `clickhouse.flush_interval_seconds`
（既定1秒）まで追加データを待ち、`batch_size`（既定1,000行）に達した場合は
待たずに送信します。ClickHouse の書き込みが成功した行だけ `shipped_at` を更新します。

送信済み行は既定で24時間保持し、10秒ごとに最大2,000行を削除します。
設定は `spool.shipped_retention_hours`、`spool.prune_interval_seconds`、
`spool.prune_batch_size` です。未送信・送信失敗行は削除しません。
削除された過去のハッシュはローカルSQLiteでは重複判定できなくなりますが、
ClickHouse の `ReplacingMergeTree` が同じキーの行を最終的に整理します
（マージ完了前の重複可視性は従来どおりあります）。

SQLite の `DELETE` はファイル容量を自動で縮小しません。空きページは再利用されます。
既存の大きなDBを縮小する場合は、以下の**停止時コピー**を使用してください。
運用中のDBに `VACUUM` をかけたり、`-wal` や `-shm` を削除しないでください。

### 既存spoolのオフライン縮小（brl.localの例）

dev.localが正常に収集中であること、ClickHouse送信が成功していて
未送信行数が少ないことを先に確認します。以下はソースコードを更新した後の操作です。
**brl.localのサービスを停止してから**実施します。

```bash
sudo systemctl stop bitflyer-realtime-lake
sudo systemctl is-active bitflyer-realtime-lake  # inactive を確認
cd /opt/bitflyer-realtime-lake
./.venv/bin/python ops/compact_spool.py \
  --source spool/events.sqlite3 \
  --destination spool/events.sqlite3.compact \
  --retention-hours 24 \
  --service-stopped
```

このツールは元DBを削除・変更・置換しません。未送信行すべてと直近24時間の
送信済み行だけを新DBにコピーし、未送信件数とSQLite整合性を検証します。
コピー先ファイルが既に存在すると失敗します。空き容量が足りない場合も元DBは保持されます。

コピーが成功したら、旧DBのWALを確実にチェックポイントしてから、
**サービスを停止したまま**同一ファイルシステム内でファイルを切り替えます。
`PRAGMA wal_checkpoint(TRUNCATE)` の戻り値の先頭が `0`（busyなし）
であることを確認してください。

```bash
cd /opt/bitflyer-realtime-lake/spool
python3 - <<'PY'
import sqlite3
with sqlite3.connect("events.sqlite3") as con:
    result = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    print("checkpoint:", result)
    assert result[0] == 0
PY
mv events.sqlite3 events.sqlite3.before-compact
for suffix in -wal -shm; do
  if [ -e "events.sqlite3${suffix}" ]; then
    mv "events.sqlite3${suffix}" "events.sqlite3.before-compact${suffix}"
  fi
done
mv events.sqlite3.compact events.sqlite3
sudo systemctl start bitflyer-realtime-lake
sudo systemctl status bitflyer-realtime-lake --no-pager
```

**コピー先の`-wal`/`-shm`が残っている場合は切替前に調査してください。**
切替後は `python -m bitflyer_realtime_lake status`、journal、
ClickHouse側のINSERT元を確認します。
`events.sqlite3.before-compact` は確認が終わるまで保持してください。
旧DBを削除しない限り、ディスク空き容量は大きく増えません。
新DBで収集を再開した後に旧DBへ戻すと、その間の新規受信データが
旧DBに存在しないため、単純な巻き戻しはしないでください。

**注意：** ClickHouseが長時間停止すると未送信データは増え続けます。
データを捨てず、かつ有限のディスク容量を絶対に超えないことは両立できません。
残容量・未送信件数の監視とアラートは引き続き必要です。
