# 🌾 dso_system (一部だけ公開)

分散型Webスクレイピング基盤（Prefect 3.x + Playwright 構成）。
クローラー開発者は **「HTMLパース（解析）ロジックの記述」だけに集中**し、面倒なインフラ処理（並列処理、定期実行、リトライ、CSV生成、データベース保存、進行状況の可視化等）はすべてシステムが全自動で引き受けます。
> **🙋‍♂️「Pythonはなんとなくわかるけど、GitとかLinuxとかはあんまり…」という方へ**  
> このページでは、難しいコマンドを使わずに、あなたのパソコン（Windows / Mac）でプログラムを動かす準備をする方法を説明します！
> わからないところがあれば、遠慮なく `docs` フォルダの中身ごとChatGPTやClaudeなどのAIに投げ込んで質問してみてください。
## わからないことろがあれば本ページの下部、`docs/はじめの一歩.md`、`docs`フォルダーを参考するか、もしくは`docs`フォルダーをまるごとAIに投げてください。AI用のプロンプトも `DevKit/.agent/SYSTEM_PROMPT.md` にあります。あとは実装例なども参考にできるかと思います

---

## 🛠 Prerequisites

本プロジェクトを開発・運用環境に構築するために必要なソフトウェア要件です。

### 開発用PC (ローカル環境)

- **Git**
- **Python 3.13以上**
- **uv** (超高速なPythonパッケージマネージャー。未導入の場合は `pip install uv`)なければpip
  
>※ コマンド入力が怖い人でも大丈夫！このあとは「コピペで貼り付けてEnter」だけで進められるように書いています。
### 本番と同様な環境で開発したい場合

- **Docker** & **Docker Compose**
- **Git**

---

## 1. 最初の準備（環境構築）

まずは、この「dso_system」を使うための準備をあなたのパソコン内で行いましょう。これらは**最初に1回だけ**やればOKです。

### 1-1. リポジトリのクローン

「ターミナル（WindowsならコマンドプロンプトやPowerShell）」という黒い画面を開いてください。
次に、あなたがプログラムを置きたいフォルダ（たとえば `Documents` や `Desktop`）に移動してから、以下のコマンドをコピー＆ペーストしてEnterを押します。

```bash
# プログラムの設計図をダウンロードする
git clone https://github.com/STREAM-inc/dso_system.git

# ダウンロードしたフォルダの中へ移動する
cd dso_system
```
※社内の「共有フォルダ」などに `dso_system` フォルダが置いてある場合は、それをコピペで持ってくるだけでも大丈夫です。

### 1-2. 必要な部品（パッケージ）のインストール
プログラムを動かすための「道具箱（パッケージ）」をパソコンにインストールします。
以下のコマンドを貼り付けて実行してください。

```bash
# uvというツールを使って道具箱をインストールします（早くておすすめです）
uv pip install -r pyproject.toml

# もし「uv なんてコマンドないよ！」と怒られたら、おなじみのこちらのコマンドでOK
pip install -r pyproject.toml
```

### 1-3. パスの設定（初回のみ・重要）

以下のコマンドを**クローン後に1回だけ**実行してください。
これにより、`scripts/sites/` 以下のスクレイピングコードを**直接実行できる**ようになります。
>これをやらないと、あとで「そのプログラム、見つかりません！」というエラーに悩まされることになります。

```bash
# これを1回だけ実行！
uv pip install -e .

# （uvがない人はこちら）
pip install -e .
```
これをしておくだけで、VS Code（エディタ）の「▶（実行）」ボタンから簡単にプログラムをテストできるようになります。

### 1-4. Playwright（プレイライト）の準備

最近のWebサイトは、そのままでは文字が取得できず「ブラウザ」を使って画面を開かないといけない場合があります。
そのための「裏で動く透明なブラウザ」をインストールします。

```bash
playwright install chromium
```

---

## 💻 2. あなたがやること（プログラムの書き方）

準備はお疲れ様でした！ここからは、あなたが実際に「情報収集プログラム」を書くまでの手順です。

### 2-1. ファイルを作る

`scripts/sites/` というフォルダの中に、新しく `.py` のファイルを作りましょう。（例： `example.py`）
この中に、「どこから文字を取得するか」だけを書いていきます。

書き方は、この説明書を読むより**もっとわかりやすいマニュアル**が用意されています！
👉 **[🔰 はじめの一歩（docs/はじめの一歩.md）](./docs/はじめの一歩.md)** を開いてみてください。
「こう書けば動くよ！」というテンプレートや、全自動システムがどう手伝ってくれるかが書かれています。

### 2-2. プログラムをテストする

プログラムが書けたら、VS Code などのエディタで「▶（実行）」ボタンを押すか、ターミナルで実行してみましょう。問題なく書けていれば、`output/` というフォルダに自動的にキレイな `CSVファイル` が出来上がるはずです！

---

## 🚀 3. 完成したら…（システムへの引き継ぎ）

さて、あなたの手元でプログラムが動いて CSV ができましたか？
**あなたの仕事はここまでです。** この先、「このプログラムを毎晩動かす」「結果をS3(クラウド)に送る」「みんなにTeamsで通知する」といった仕上げ・運用業務は「基盤システム」が自動でやります。

システムにあなたの作ったプログラムを認識させるための手順は以下の2つだけです。

### 3-1. `dso_system-Scripts` にアップロードする
あなたの作ったプログラムファイル（例： `example.py`）を、スクリプト専用のGitリポジトリ **[`dso_system-Scripts`](https://github.com/STREAM-inc/dso_system-Scripts)** の正しいフォルダ（`sites/` の中など）にアップロード（Push）してください。

### 3-2. `sites.yml` に設定を書き込む
同じく `dso_system-Scripts` フォルダの中にある `sites.yml` という設定ファイルを開き、一番下にあなたのサイトの設定を追記します。

```yaml
  - site_id: example              # 英語でわかりやすい名前
    name: 株式会社◯〇◯             # 日本語のサイト名（これがCSVファイルの名前になります）
    module: portal.example        # プログラムの場所（sites/portal/example.py なら portal.example）
    url: https://example.com      # スクレイピングするURL
    schedule: "0 3 1 * *"         # いつ動かすか（cron式という書き方で指定します）
    enabled: true                 # true なら有効、false なら無効
```

💡 **「cron式」ってなに？**
「いつ動かすか」をシステムに伝えるための式で、左から順に `分` `時` `日` `月` `曜日` を表します。
よくわからなくても大丈夫！以下のよくあるパターンをコピペして使ってください。
- **毎日 夜中の2時**に動かす： `"0 2 * * *"`
- **毎週 月曜の朝5時**に動かす： `"0 5 * * 1"`
- **毎月 1日の朝3時**に動かす： `"0 3 1 * *"`
> （もっと細かく設定したい場合は、ChatGPTなどのAIに「毎週金曜の18時に動かしたいからcron式を教えて」と聞くと一発で教えてくれます！）

これを書き込んでアップロードするだけで、翌日から自動的に全自動ロボットが働き始めます！

実行後、`output/` ディレクトリ配下に `YYYYMMDD_テストサイト.csv` が正しく出力されていれば、クローラーの開発は完了です！

---

## 📚 4. 困ったときの説明書ガイド（ドキュメント一覧）

`docs/` フォルダには、用途に合わせて様々な説明書が用意されています。自分の目的やレベルに合わせて読んでみてください。

### 🟢 はじめての方・クローラーを作る方（最優先！）
- **[🔰 はじめの一歩（docs/はじめの一歩.md）](./docs/はじめの一歩.md)**
  プログラムの書き方、ルールの基本がすべて書かれています。**まずはこれから！**
- **[💡 実装例（docs/実装例.py）](./docs/実装例.py)**
  実際のコードの書き方のお手本です。コピペして作り始めるのにとても便利です。
- **[❓ FAQ（docs/FAQ.md）](./docs/FAQ.md)**
  「よくあるエラー」や「こういう時どう書くの？」といった疑問と解決策をまとめています。

### 🟡 もう少し詳しく仕組みを知りたい方（中級者向け）
- **[📖 開発マニュアル（docs/開発マニュアル.md）](./docs/開発マニュアル.md)**
  準備されている便利な関数（メソッド）の一覧や、データ出力の詳しいルールが書かれています。
- **[⚙️ 詳細解説_Framework（docs/詳細解説_Framework.md）](./docs/詳細解説_Framework.md)**
  `StaticCrawler` や `DynamicCrawler` の裏側でどんな処理（自動リトライなど）が動いているかの解説です。

### 🔴 本番環境を管理する方（インフラ担当・管理者向け）
> ※ここはクローラーを作るだけなら読まなくても大丈夫です！
- **[🚀 運用マニュアル_スクリプト自動同期（docs/運用マニュアル_スクリプト自動同期.md）](./docs/運用マニュアル_スクリプト自動同期.md)**
  本番サーバー上で、どのようにプログラムが自動更新されるかの仕組みです。
- **[🏗 詳細解説_統括層（docs/詳細解説_統括層.md）](./docs/詳細解説_統括層.md)**
  Prefect（管理システム）や Docker などの本番運用システム全体についての詳細なアーキテクチャ解説です。
- **[📝 本番運用に向けた課題（docs/本番運用に向けた課題.md）](./docs/本番運用に向けた課題.md)**
  システム管理者が今後対応すべきインフラ側の課題リストです。


---
---
---

## もし本番環境 (Docker + Prefect) と同様な環境で開発したい場合(PC09以外でやってください)

同じ `docker-compose.yml` を使い回す設計になっています。開発機（ローカル）で動かすか、運用機（本番）で動かすかの違いは **`.env` ファイルの設定（IPアドレス）だけ** です。

## 1. 環境変数のセットアップ

`DevKit/.env`をルートディレクトリにコピーしてください。

`.env` ファイルを開き、自身の環境に合わせて設定を変更します。

- **`ORCHESTRATOR_IP`**: 外部にPrefectサーバーがある場合はそのIPを。ローカル完結させる場合は削除かコメントアウト（デフォルトで `localhost` になります）。
- **`dso_system_ENV`**: システムの動作モード（安全装置）です。
  - `dev` : テストモード。テスト用キューで動きます。（**※ローカル開発時は必ずこれにしてください**）
  - `prod` : 本番モード。本番用キューで動きます。本番サーバー以外では使用しないでください。

## 2. Dockerコンテナの起動

以下のコマンドで、PostgreSQL、Prefect Server（管理画面）、実行ワーカーがすべて起動します。

```bash
# 全システムを起動する場合（管理サーバー用PC）
docker compose --profile server --profile worker up -d --build
```

## 3. run_flow.py を通してテストする

本番環境（Prefectの監視下）でクローラーが正常に動くか手動でテストするには、ワーカーコンテナの中で `run_flow.py` を実行します。

```bash
# 例: site_id が "site_name" の場合
docker compose exec worker python bin/run_flow.py --site-id site_name
# ※実行には、事前に `sites.yml` への追記が必要です。
```

```bash
# もし`sites.yml`を書いていない場合（全パラメータを手動で指定）
docker compose exec worker python bin/run_flow.py \
  --site-id site_name \
  --name "日本語サイト名" \
  --module "site_name" \
  --url "https://example.com"
```

完走すると `output/` にCSVが生成され、ブラウザのPrefectダッシュボードにも実行履歴が記録されます。

## 4. サイトカタログへの登録とデプロイ

1. `sites.yml` を開き、新規作成したクローラーの設定（ID、モジュールパス、URL等）を追記します。
2. ターミナルから以下のコマンドを実行し、変更内容を Prefect Server に自動登録（Deployment）します。

```bash
# Workerコンテナ内でデプロイ用スクリプトを実行
docker compose exec worker python bin/deploy.py
```

### 4-1. deploy.py が自動で行うこと

`deploy.py` を実行すると、以下が自動で処理されます。

| 処理 | 内容 |
| ---- | ---- |
| **新規登録・更新** | `enabled: true` の全サイトのデプロイメントを Prefect に登録 |
| **初回自動投入** | 初めて登録されたサイトのフローランをワークプールに即時投入 |
| **古いデプロイ削除** | `sites.yml` から削除・無効化されたサイトのデプロイメントを自動削除 |

`sites.yml` からサイトを削除・`enabled: false` にした後も `deploy.py` を再実行すれば、ダッシュボードから自動で消えます。

### 4-2. ダッシュボードの確認

ブラウザから `http://localhost:4200` にアクセスします。
Prefect ダッシュボードの左メニュー「Deployments」から登録状況を確認できます。実行時のログや進捗バー（ETA）もこの画面から確認できます。

---

## システムの停止とトラブルシューティング

### 開発・運用の停止

```bash
# 起動中の全コンテナを停止する（データは保持されます）
docker compose --profile server --profile worker down
```

---

# dso_system クローラー開発 はじめの一歩

> **対象:** スクレイピング未経験でもOK。Python の基礎（変数、for文、関数）がわかれば大丈夫です。

---

## このシステムは何をしてくれるの？

普通、Webサイトからデータを集めるには、こういうコードを全部自分で書く必要があります:

```
通信処理 → HTML取得 → データ抽出 → エラー対策 → CSV保存 → ファイル名生成 → 文字コード対策 → ...
```

**dso_system では、あなたが書くのは「データ抽出」の部分だけ。** 残りは全てシステムが自動でやります。

---

## あなたが書くこと vs システムがやること

| やること | 誰がやる？ |
|---------|-----------|
| 「このHTML要素からデータを取り出す」というロジック | **あなた** |
| それ以外すべて（↓） | **システムが自動** |
| - Webサイトにアクセスする（通信） | 自動 |
| - 通信エラー時に3回やり直す（リトライ） | 自動 |
| - エラー発生時にスキップして継続（エラー耐性） | 自動 |
| - ブラウザの起動と終了 | 自動 |
| - CSVファイルの作成と保存 | 自動 |
| - 電話番号の全角→半角変換 | 自動 |
| - Null（空っぽのデータ）の処理 | 自動 |
| - ログ（実行記録）の出力 | 自動 |
| - 進捗表示（あと何分かかるか） | 自動 |

---

## 用語解説（先に読んでおくと楽です）

| 用語 | 意味 |
|------|------|
| **`parse()`** | あなたが書く関数。「HTMLを解析してデータを取り出す処理」のこと。英語で「parse = 解析する」。この関数の中身だけがあなたの仕事 |
| **`yield`** | Python の機能。「データを1件ずつ返す」という意味。`return` に似ているが、何度でも呼べる。`yield { ... }` と書くと、システムが自動でCSVに書き込んでくれる |
| **`Schema`** | カラム名（「名称」「TEL」「住所」など）の定数集。タイプミスを防ぐために文字列ではなく `Schema.NAME` のような定数を使う |
| **`soup`** | HTMLを解析しやすい形に変換したオブジェクト。`self.get_soup(url)` で取得できる。`soup.select(".class名")` で要素を検索できる |
| **`StaticCrawler`** | 普通のWebサイト用の基底クラス。あなたのクラスはこれを継承する |
| **`DynamicCrawler`** | JavaScriptで描画されるサイト用。ヘッドレスブラウザ（見えないブラウザ）で実際にページを開く |

---

## あなたが変えてよい場所（全部で4箇所だけ）

```python
class MyScraper(StaticCrawler):

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ① DELAY（数値を変えてOK）
    #    → 1件取得するごとに何秒待つか。
    #    → サーバーに負荷をかけすぎないための設定。
    #    → 1.0 = 1秒待つ。0.5 = 0.5秒。0 = 待たない。
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    DELAY = 1.0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ② EXTRA_COLUMNS（必要な場合だけ追加）
    #    → システムに最初から用意されていないカラム名を使いたい時に宣言する。
    #    → 例: そのサイトにしかない「施術ジャンル」「予約方法」など。
    #    → 使わないなら空のまま [] でOK。
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    EXTRA_COLUMNS = ["施術ジャンル", "予約方法"]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ⑤ CONTINUE_ON_ERROR（通常は変更不要）
    #    → True（デフォルト）: 404やタイムアウト等のエラーが起きてもスキップして次へ進む。
    #    → False: エラーが起きたら即座に停止する（厳密なデータ取得が必要な場合）。
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CONTINUE_ON_ERROR = False  # ← コメントを外すとエラー時に即停止

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ③ parse() メソッドの中身（ここがあなたの仕事の全て）
    #    → 対象サイトに合わせたスクレイピングロジックを書く。
    #    → yield { ... } でデータを1件ずつ返す。
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def parse(self, url: str):
        soup = self.get_soup(url)
        items = soup.select(".shop-card")  # ← 対象サイトのHTML構造に合わせて変える

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ④ self.total_items（任意。セットすると進捗表示が出る）
        #    → 全部で何件あるかをセットすると、「残り何分」が自動で表示される。
        #    → わからなければ書かなくてOK（書かなくても動く）。
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        self.total_items = len(items)

        for item in items:
            yield {
                Schema.NAME: item.select_one(".name").get_text(strip=True),
                Schema.TEL:  item.select_one(".tel").get_text(strip=True),
            }
```

**上記の ①〜⑤ 以外は必要なければ基本的にしなくていい。**

---

## 最初の1歩: 実際にやってみよう

### Step 1: ファイルを作る

`scripts/sites/` の中に、好きな名前でフォルダと `.py` ファイルを作ります。

```
scripts/sites/
└── portal/           ← カテゴリ名（自由に作ってOK）
    └── my_site.py    ← あなたのファイル
```

### Step 2: 以下をコピペして、コメントの指示に従って書き換える

```python
# scripts/sites/portal/my_site.py

# ===== ここから下の3行はコピペするだけ（お約束） =====
import sys
from pathlib import Path
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
# ===== ここまでお約束 =====

from src.framework.static import StaticCrawler   # 普通のサイトならこれ
from src.const.schema import Schema               # カラム名の定数

class MySiteScraper(StaticCrawler):
    """ここにサイトの説明を書く"""

    DELAY = 1.0  # ← 待機秒数。1.0 = 1秒

    def parse(self, url: str):
        # 1. HTMLを取得する（この1行で通信・リトライ・エラー処理は全部終わり）
        soup = self.get_soup(url)

        # 2. データを探す（CSSセレクタは対象サイトに合わせて変える）
        items = soup.select(".shop-card")

        # 3. 進捗表示を有効化（任意。なくても動く）
        self.total_items = len(items)

        # 4. 1件ずつデータを返す
        for item in items:
            yield {
                Schema.NAME: item.select_one(".name").get_text(strip=True),
                Schema.ADDR: item.select_one(".address").get_text(strip=True),
                Schema.TEL:  item.select_one(".tel").get_text(strip=True),
                Schema.URL:  url,
            }

# ===== ここから下もコピペするだけ（実行用） =====
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    MySiteScraper().execute("https://ここに対象サイトのURLを入れる")
```

### Step 3: 実行する

VSCode の再生ボタン ▷ を押すか、ターミナルで:

```bash
python scripts/sites/portal/my_site.py
```

### Step 4: 結果を確認する

`output/` フォルダに CSV ファイルが自動生成されます。Excel で開いて中身を確認してください。

---

## 使えるカラム名の一覧

`Schema.○○` で使えるカラム名の完全な一覧は `src/const/schema.py` を開いて確認してください。
よく使うものだけ抜粋すると:

| 書き方 | CSVに出力される名前 | 用途 |
|--------|-------------------|------|
| `Schema.NAME` | 名称 | 会社名・店舗名 |
| `Schema.ADDR` | 住所 | 住所 |
| `Schema.TEL` | TEL | 電話番号（全角→半角は自動） |
| `Schema.HP` | HP | ホームページURL |
| `Schema.URL` | 取得URL | データを取得したページのURL |
| `Schema.PREF` | 都道府県 | 都道府県 |
| `Schema.LOB` | 事業内容 | 事業内容 |
| `Schema.HOLIDAY` | 定休日 | 定休日 |
| `Schema.TIME` | 営業時間 | 営業時間 |
| `Schema.CAT_SITE` | サイト定義業種・ジャンル | そのサイト独自の業種分類 |

全カラム → `src/const/schema.py` を参照

---

## Q&A

### Q. ページ送り（次のページ）はどう書くの？

`parse()` の中でページをループするだけです。

```python
def parse(self, url: str):
    page_num = 1
    while True:
        soup = self.get_soup(f"{url}?page={page_num}")
        items = soup.select(".item")
        if not items:
            break  # データがなくなったら終了
        for item in items:
            yield { Schema.NAME: item.get_text(strip=True) }
        page_num += 1
```

### Q. 1件だけ取得に失敗しても全体を止めたくない

**デフォルトで自動対応されます。** `CONTINUE_ON_ERROR = True`（デフォルト）の場合、`get_soup()` の通信エラーやアイテム処理中のエラーは自動的にスキップされ、次の処理に進みます。スキップされたエラー件数は完了時にログに表示されます。

もし `parse()` 内で独自のエラー処理を追加したい場合は、`try/except` で囲んで `continue` することもできます:

```python
for item in items:
    try:
        yield { Schema.NAME: item.select_one(".name").text }
    except Exception as e:
        self.logger.warning("スキップしました: %s", e)
        continue
```

### Q. JavaScriptで描画されるサイトはどうする？

`StaticCrawler` を `DynamicCrawler` に変えるだけ。`self.get_soup(url)` で StaticCrawler と同じように使えます（エラー耐性も自動適用）。

```python
from src.framework.dynamic import DynamicCrawler

class MyScraper(DynamicCrawler):
    def parse(self, url: str):
        soup = self.get_soup(url)  # Playwright経由でHTMLを取得（エラー時はNone）
        if soup is None:
            return
        # あとは同じように soup.select() して yield する
```

より細かいブラウザ制御が必要な場合は `self.page` を直接操作することもできます。

### Q. 既存のクローラーを参考にしたい

`scripts/sites/` 内の既存ファイルを見てください。実際に動いているコードが一番の教材です。

### Q. 詳しい技術仕様を知りたい

`docs/開発マニュアル.md` に全メソッドの詳細が書いてあります。
