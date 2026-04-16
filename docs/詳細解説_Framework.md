# dso_system フレームワーク 詳細解説 Ver.0.1.0

> **対象読者:** Python を大学で 48 時間ほど学習済み。アルゴリズム・Linux・DB の基礎知識あり。システム開発は未経験。
>
> **ゴール:** この文書を読み終えたら、全てのファイルを**一から自分で書き直せる**こと。

---

## 目次

1. [このシステムは何をするのか？](#1-このシステムは何をするのか)
2. [フォルダ構成と各ファイルの役割](#2-フォルダ構成と各ファイルの役割)
3. [前提知識: 使っている Python の仕組み](#3-前提知識-使っている-python-の仕組み)
4. [Step 1: Schema — カラム名の辞書を作る](#4-step-1-schema--カラム名の辞書を作る)
5. [Step 2: normalizer — データの前処理工場](#5-step-2-normalizer--データの前処理工場)
6. [Step 3: ItemPipeline — データの受付窓口](#6-step-3-itempipeline--データの受付窓口)
7. [Step 4: BaseCrawler — 指揮者(フロー管理)](#7-step-4-basecrawler--指揮者フロー管理)
8. [Step 5: StaticCrawler / DynamicCrawler — 道具の違い](#8-step-5-staticcrawler--dynamiccrawler--道具の違い)
9. [全体の処理フロー: 一連の流れを追う](#9-全体の処理フロー-一連の流れを追う)
10. [設計原則: なぜこう分けたのか？](#10-設計原則-なぜこう分けたのか)
11. [実際にスクレイパーを作る手順](#11-実際にスクレイパーを作る手順)
12. [よくある疑問 Q&A](#12-よくある疑問-qa)
13. [全自動データカタログ機能](#13-全自動データカタログ機能)

---

## 1. このシステムは何をするのか？

Web サイトから企業情報（会社名、電話番号、住所 など）を自動で集めて CSV に保存するシステムです。

**問題:** サイトごとにスクレイピングコードを書くと、毎回同じようなコード（CSV 保存、エラー処理、待機処理 など）を繰り返し書くことになります。

**解決策:** 「共通部分」はフレームワーク側が一度だけ書き、開発者は「そのサイト固有のロジック」だけを書けばいいようにしました。

```text
開発者が書くコード:
  「このサイトでは、h1タグが会社名で、spanタグが電話番号」 ← これだけ！

フレームワークが自動でやること:
  CSVファイルの作成、電話番号の全角→半角変換、エラー処理、待機時間の挿入...
```

---

## 2. フォルダ構成と各ファイルの役割

```bash
dso_system/
├── src/
│   ├── const/
│   │   └── schema.py          ← ① カラム名の定義（「名称」「TEL」など）
│   ├── utils/
│   │   └── normalizer.py      ← ② データの正規化（全角→半角 など）
│   └── framework/
│       ├── __init__.py        ← パッケージの公開設定
│       ├── pipeline.py        ← ③ データの検証→正規化→CSV保存
│       ├── base.py            ← ④ 実行フローの制御（指揮者）
│       ├── static.py          ← ⑤a 静的サイト用ツール (Requests)
│       └── dynamic.py         ← ⑤b 動的サイト用ツール (Playwright)
├── scripts/
│   └── sites/                 ← ⑥ 開発者がサイトごとに書くコード
├── output/                    ← CSV の出力先
└── docs/
    └── 設計書.md
```

**①〜⑤ がフレームワーク（共通基盤）、⑥ が開発者が書く部分です。**

各ファイルの依存関係を矢印で表すと:

```bash
⑥ 開発者コード (scripts/sites/xxx.py)
  └──→ ⑤ StaticCrawler または DynamicCrawler  (道具を選ぶ)
         └──→ ④ BaseCrawler                    (実行フローの制御)
                └──→ ③ ItemPipeline             (データ処理)
                       ├──→ ① Schema            (カラム名の辞書)
                       └──→ ② normalizer        (正規化ルール)
```

> **ポイント:** 矢印の先にあるファイルは、矢印の元のファイルの存在を**知りません**。
> これが「疎結合（低い結合度）」です。`normalizer.py` は `BaseCrawler` の存在を知らないし、
> `BaseCrawler` は `normalizer.py` の存在を知りません（Pipeline 経由で間接的に使っているだけ）。

---

## 3. 前提知識: 使っている Python の仕組み

このフレームワークでは、大学の授業ではあまり扱わない Python の機能をいくつか使っています。
コードを読む前に、ここで予習しておきましょう。

### 3.1 抽象クラス (`abc.ABC`, `@abc.abstractmethod`)

**「このメソッドは子クラスで必ず実装してね」** という約束を強制する仕組みです。

```python
import abc

class Animal(abc.ABC):          # abc.ABC を継承すると「抽象クラス」になる
    @abc.abstractmethod         # この飾り（デコレータ）で「必ず実装してね」と指定
    def speak(self):
        pass

class Dog(Animal):
    def speak(self):            # ← 実装した。OK!
        return "ワン"

class Cat(Animal):
    pass                        # ← speak() を実装していない

dog = Dog()                     # ← OK
cat = Cat()                     # ← エラー! "Can't instantiate abstract class Cat"
```

**このシステムでの使い方:**

- `BaseCrawler` は抽象クラス。`parse()` メソッドが `@abstractmethod`。
- 開発者は `parse()` を必ず実装しないとインスタンス化できない。

### 3.2 `@final` デコレータ

**「このメソッドは子クラスで上書きしないでね」** という意思表示です。

```python
from typing import final

class Base:
    @final
    def execute(self):       # 「execute は上書き禁止」
        print("共通処理")

class Child(Base):
    def execute(self):       # ← 型チェッカー（mypy等）が警告を出す
        print("勝手に変更")
```

> `@abstractmethod` = 「必ず実装して」
> `@final` = 「絶対に上書きしないで」
> 真逆の意味です。

### 3.3 `__init_subclass__` — クラス定義時に自動実行 {#init-subclass}

これは少し難しいですが、非常に重要です。

普通のメソッドは「インスタンスを作ったとき」や「メソッドを呼んだとき」に動きます。
`__init_subclass__` は **「子クラスが定義されたとき」** に自動で動きます。

```python
class Base:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        print(f"子クラス {cls.__name__} が定義されました!")

class Child(Base):      # ← この行を書いた瞬間に、上の print が実行される！
    pass
#=> "子クラス Child が定義されました!"
```

**このシステムでの使い方:**
開発者が `execute()` や `__init__()` を勝手に上書きしていないかを、
クラスが定義された瞬間――**コードを実行する前**――にチェックしています。
つまり「実行してからエラーになる」のではなく、「定義した時点でエラーになる」ので安全です。

```python
# cls.__dict__ には「そのクラス自身で定義したメソッド」だけが入っている
# （親クラスから継承したものは入らない）
if "execute" in cls.__dict__:   #  execute を "自分で" 定義してたら → 違反！
    raise TypeError("execute は上書き禁止です！")
```

### 3.4 ジェネレータ (`yield`)

普通の関数は `return` で値を1つ返して終了しますが、
`yield` を使うと **値を1つ返すたびに一時停止** し、次に呼ばれたら再開します。

```python
def count_up():
    yield 1        # ← 1 を返して一時停止
    yield 2        # ← 次に呼ばれたら 2 を返して一時停止
    yield 3        # ← 次に呼ばれたら 3 を返して終了

for num in count_up():
    print(num)
#=> 1
#=> 2
#=> 3
```

**このシステムでの使い方:**
`parse()` メソッドは `yield` でデータを1件ずつ返します。
10万件のデータがあっても、メモリに全部載せずに1件ずつ処理できます。

Pythonが yield で一時停止して、Pipelineで文字を置換してCSVに書き込む処理は、0.001秒もかかりません。
一方で、次のページを取得するための通信や待機時間には 1〜3秒 かかるので、問題なし。

```python
def parse(self, url):
    for page in range(1, 100):
        soup = self.get_soup(f"{url}?page={page}")
        for row in soup.select("tr"):
            yield {                      # ← 1件返すたびに一時停止
                Schema.NAME: row.text,
                Schema.TEL: "03-1234-5678",
            }
            # ↑ この1件が Pipeline で検証 → 正規化 → CSV保存される
            # 次の yield まで、次の行の処理に進む
```

### 3.5 `logging` モジュール — `print()` の上位互換

`print()` でもメッセージは出せますが、`logging` を使うと **レベル分け** ができます。

```python
import logging
logger = logging.getLogger(__name__)   # このファイル用の logger を取得

logger.debug("これは開発者向けの詳細情報")     # 通常は非表示
logger.info("正常な処理の報告")                 # 通常表示
logger.warning("注意: 軽微な問題が発生")       # 黄色で表示されることが多い
logger.error("エラー: 深刻な問題が発生")       # 赤で表示されることが多い
```

テスト時は `debug` レベルまで表示し、本番では `warning` 以上だけ表示、のように切り替えられます。
`print()` だと全部表示するか全部消すかしかありません。

### 3.6 `Path` オブジェクト — ファイルパスの安全な操作

```python
from pathlib import Path

# 文字列の結合ではなく / 演算子でパスを組み立てる
path = Path("/Users/eigou/Desktop") / "dso_system" / "output"
# => /Users/eigou/Desktop/dso_system/output

path.mkdir(parents=True, exist_ok=True)  # フォルダが無ければ作る、あればスキップ
```

### 3.7 `frozenset` — 変更不可の集合

```python
allowed = frozenset(["名称", "TEL", "住所"])   # 一度作ったら変更できない set
"TEL" in allowed    # => True   (検索が高速: O(1))
"XXXX" in allowed   # => False
```

通常の `set` と違い、中身を追加・削除できません。
「このリストは絶対に変わらない」ことをコードで表現できます。

---

## 4. Step 1: Schema — カラム名の辞書を作る

**ファイル:** `src/const/schema.py`

```python
class Schema:
    NAME = "名称"          # 会社名、店舗名など
    PREF = "都道府県"
    ADDR = "住所"
    TEL  = "TEL"           # 電話番号
    HP   = "HP"            # ホームページURL
    URL  = "取得URL"       # データを取得した元ページのURL
    # ... 他にも多数
```

### なぜこれが必要？

想像してみてください。開発者 A さんが `"電話番号"` というキー名で保存し、
開発者 B さんが `"TEL"` で保存したら、CSVのカラム名がバラバラになります。

```python
# X 悪い例（キー名がバラバラ）
yield {"電話番号": "03-1234-5678"}  # A さん
yield {"TEL": "03-1234-5678"}       # B さん
yield {"tel": "03-1234-5678"}       # C さん

# O 良い例（Schema 定数を使う → 打ち間違いも防げる）
yield {Schema.TEL: "03-1234-5678"}   # 全員統一
```

`Schema.TEL` と書けば、実際の値は `"TEL"` です。
もし将来「カラム名を `電話番号` に変えたい」と思ったら、
`Schema` クラスの `TEL = "TEL"` を `TEL = "電話番号"` に変えるだけで、
全てのスクレイパーの出力が一斉に変わります。

### 仕組みの詳細

`Schema` は「定数の入れ物」です。メソッドも `__init__` も無い、ただの入れ物です。
Python では **クラス変数** と呼ばれます。

```python
Schema.NAME   # => "名称"
Schema.TEL    # => "TEL"
```

後の工程で「開発者が使ったキー名が Schema に存在するか？」をチェックします。
存在しないキー名を使ったら即エラーにして、タイプミスを防ぎます。

---

## 5. Step 2: normalizer — データの前処理工場

**ファイル:** `src/utils/normalizer.py`

### normalizerは何をするファイル？

スクレイピングで取ってきたデータは**汚い**ことが多いです。

```text
取得した電話番号: "０３−１２３４−５６７８"   ← 全角！ハイフンも全角！
欲しい形:        "03-1234-5678"            ← 半角に統一
```

こういった「データのお掃除」を **カラムの種類に応じて自動で行う** のが normalizer です。

### コードの詳細解説

```python
import re
from src.const.schema import Schema
```

- `re` は正規表現モジュール。文字のパターンマッチと置換に使います。
- `Schema` からカラム名の定数を使います（`Schema.TEL` など）。

```python
def _normalize_tel(value: str) -> str:
    """TEL: 全角→半角、数字とハイフン以外を除去"""
    table = str.maketrans("０１２３４５６７８９－", "0123456789-")
    return re.sub(r"[^\d\-]", "", str(value).translate(table))
```

この関数は3段階で電話番号を綺麗にします:

1. `str.maketrans(...)` — 全角と半角の変換テーブルを作る
   - `"０"` → `"0"`, `"１"` → `"1"`, ... `"－"` → `"-"`
2. `.translate(table)` — テーブルに従って全角を半角に変換する
3. `re.sub(r"[^\d\-]", "", ...)` — 数字(`\d`)とハイフン(`\-`)以外の文字を全て削除する

```text
入力:  "（０３）１２３４ー５６７８"
Step1: (変換テーブル作成)
Step2: "(03)1234-5678"           ← 全角→半角
Step3: "03-1234-5678"            ← カッコやスペースを除去
```

```python
_NORMALIZERS = {
    Schema.TEL: _normalize_tel,
}
```

**これが核心です。** カラム名と正規化関数の対応表（辞書）です。

- `Schema.TEL`（= `"TEL"`）のデータが来たら → `_normalize_tel` 関数を使え
- 対応表に登録されていないカラムはそのまま素通りさせる

**拡張方法:** 将来「住所」の正規化が必要になったら:

```python
def _normalize_addr(value: str) -> str:
    # 住所の全角→半角変換など
    ...

_NORMALIZERS = {
    Schema.TEL: _normalize_tel,
    Schema.ADDR: _normalize_addr,   # ← 追加するだけ！
}
```

`base.py` も `pipeline.py` も**一切変更する必要がありません**。
（これが「開放閉鎖原則」です:「拡張に対して開いている、変更に対して閉じている」）

```python
def normalize(key: str, value) -> any:
    if value is None:
        return ""

    normalizer = _NORMALIZERS.get(key)   # 辞書から関数を取得（なければ None）
    if normalizer:
        return normalizer(value)         # 関数が見つかったら実行
    return value                          # 見つからなければそのまま返す
```

外部から呼ばれる唯一の関数です。

- `_NORMALIZERS.get(key)` で辞書から正規化関数を探す
- 見つかれば実行、見つからなければ値をそのまま返す
- `None` は空文字列 `""` に変換する（CSV に `None` と書かれるのを防ぐ）

---

## 6. Step 3: ItemPipeline — データの受付窓口

**ファイル:** `src/framework/pipeline.py`

### pipelineは何をするファイル？

Crawler が `yield` したデータ（辞書）を **受け取って → チェックして → 綺麗にして → CSV に書く** 一連の流れがこのクラスの仕事です。

```python
Crawler が yield した辞書
  │
  ▼
ItemPipeline.process_item()
  ├── 1. _validate_and_normalize()  ← Schema + EXTRA_COLUMNS チェック & 正規化
  └── 2. _write_to_csv()            ← CSV に1行追記
```

### pipelineコードの詳細解説

#### モジュールレベルの定数

```python
from src.const.schema import Schema
from src.utils.normalizer import normalize

_ALLOWED_COLUMNS = frozenset(
    v for k, v in vars(Schema).items() if not k.startswith("_")
)
```

ここで「許可されたカラム名の一覧」を作っています。

- `vars(Schema)` は `Schema` クラスの全属性を辞書として返します:

  ```python
  {"NAME": "名称", "TEL": "TEL", "ADDR": "住所", "__module__": "src.const.schema", ...}
  ```

- `if not k.startswith("_")` で、`__module__` 等の Python 内部属性を除外
- 残った値（`"名称"`, `"TEL"`, `"住所"`, ...）で `frozenset` を作る

結果: `_ALLOWED_COLUMNS = frozenset({"名称", "TEL", "住所", "都道府県", ...})`

> **なぜモジュールレベル？（クラスの外に書く理由）**  
> `_ALLOWED_COLUMNS` はプログラム実行中に変わりません。
> `__init__` の中に書くと、インスタンスを作るたびに毎回同じ計算をしてしまいます。
> モジュールレベル（クラスの外）に書けば、**ファイル読み込み時に1回だけ** 計算されます。

#### `__init__` — 初期化 {#inicialise}

```python
def __init__(self, output_dir: Path, crawler_name: str, extra_columns: list = None):
    self._output_dir = output_dir          # CSV の保存先フォルダ
    self._crawler_name = crawler_name      # ファイル名に使う名前

    # Schema カラム + クローラーが宣言したカスタムカラムの和集合
    self._extra_columns = list(extra_columns or [])
    self._allowed = _ALLOWED_COLUMNS | frozenset(self._extra_columns)

    # Schema カラムの定義順序（CSV ヘッダーの並び順に使用）
    self._schema_col_order = [v for k, v in vars(Schema).items() if not k.startswith("_")]

    # 実行中に観測されたカラム名を蓄積する（データカタログ用）
    self._observed_columns: set[str] = set()
    self._item_count: int = 0

    # バッファ方式: アイテムをメモリに蓄積し、close() 時に
    # 実際に観測されたカラムだけで CSV を書き出す
    self._items: list[dict] = []
    self.output_filepath = None  # 完成後のファイルパス
```

**バッファ方式:**
アイテムはメモリに蓄積され、`close()` 呼び出し時に一括で CSV に書き出されます。
これにより、**実際に観測されたカラムだけ**を CSV ヘッダーにできます。
開発者はヘッダーのことを一切意識せず、`yield` するだけでそのサイトに存在するカラムだけが CSV に出力されます。

**`self._allowed` — カスタムカラム対応:**
`_ALLOWED_COLUMNS`（Schema から自動生成）と `extra_columns`（クローラーが `EXTRA_COLUMNS` で宣言したリスト）の **和集合（union）** を `frozenset` で作ります。

```python
# 例: EXTRA_COLUMNS = ["駐車場", "営業時間"] の場合
self._allowed = frozenset({"名称", "TEL", "住所", ...})  # ← Schema
               | frozenset({"駐車場", "営業時間"})       # ← カスタム
# 結果: frozenset({"名称", "TEL", "住所", ..., "駐車場", "営業時間"})
```

`extra_columns` が `None`（デフォルト = EXTRA_COLUMNS 未宣言）なら `frozenset([])` = 空集合なので、従来通り Schema カラムだけが許可されます。

> **`or []` の意味:**
> `extra_columns or []` は Python の短絡評価です。
> `extra_columns` が `None` の場合 → `[]`（空リスト）に置き換わります。
> これにより `frozenset(None)` によるエラーを防ぎます。
>
> **アンダースコア `_` の意味:**
> `self._csv_file` のように先頭に `_` が付いた変数は、
> 「このクラスの内部でだけ使う。外から直接触らないでね」という慣習です。
> Python は**強制はしません**が、「触るな」というサインです。
> `self.output_filepath` には `_` が無い → 外部から読んで OK。

#### `process_item()` — 公開メソッド

```python
def process_item(self, item: dict) -> dict:
    clean_item = self._validate_and_normalize(item)   # 1. チェック & 正規化
    self._observed_columns.update(clean_item.keys())   # 2. カラム名を記録（カタログ用）
    self._item_count += 1                              # 3. 件数カウント
    self._write_to_csv(clean_item)                     # 4. CSV に書き込み
    return clean_item
```

外から呼ばれるメソッドは基本的にこれ1つだけ。シンプルです。

#### `_validate_and_normalize()` — 検証 & 正規化

```python
def _validate_and_normalize(self, item: dict) -> dict:
    clean_item = {}
    for key, value in item.items():
        if key not in _ALLOWED_COLUMNS:              # Schema にないキー → エラー！
            raise ValueError(f"禁止されたカラム名です: '{key}'")
        clean_item[key] = normalize(key, value)      # normalizer.py に委譲
    return clean_item
```

開発者が `yield {"名前": "テスト"}` と書いたとき:

- `"名前"` は `self._allowed` に入っていない（正しくは `"名称"`）
- → `ValueError` が発生 → **開発中にタイプミスに気づける**

正しい `{"名称": "テスト", "TEL": "０３−１２３４−５６７８"}` の場合:

- `"名称"` → そのまま（normalizer に登録なし）
- `"TEL"` → `_normalize_tel()` が適用 → `"03-1234-5678"` に変換

カスタムカラム `{"駐車場": "あり"}` の場合（`EXTRA_COLUMNS = ["駐車場"]` と宣言済み）:

- `"駐車場"` → `self._allowed` に入っている → OK
- normalizer に登録なし → そのまま `"あり"` として通過

#### `_write_to_csv()` — CSV への書き込み

```python
def close(self):
    if not self._items:
        return

    # 観測されたカラムだけで、Schema 定義順 + EXTRA 順にヘッダーを構成
    observed = self._observed_columns
    fieldnames = [c for c in self._schema_col_order if c in observed]
    fieldnames += [c for c in self._extra_columns if c in observed]

    filename = f"{datetime.now().strftime('%Y%m%d')}_{self._crawler_name}.csv"
    self.output_filepath = str(self._output_dir / filename)

    with open(self.output_filepath, mode="w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for item in self._items:
            writer.writerow(item)

    self._items.clear()
```

`close()` は `execute()` の `finally` ブロックから呼ばれます。
バッファに蓄積された全アイテムを、**実際に観測されたカラムだけ**で CSV に書き出します。
そのサイトが使っていないカラムは一切出力されません。

**2回目以降** (`self._csv_file` が存在する):

- `if not self._csv_file` が `False` なので、ファイル作成をスキップ
- データを1行追記するだけ

#### `close()` — リソース解放

```python
def close(self):
    if self._csv_file:
        self._csv_file.close()
        self._csv_file = None
        self._csv_writer = None
```

ファイルを最後に閉じます。`open()` したら必ず `close()` する。これは鉄則です。

---

## 7. Step 4: BaseCrawler — 指揮者(フロー管理)

**ファイル:** `src/framework/base.py`

### baseは何をするファイル？

オーケストラの指揮者のように、**「何を」「どの順番で」やるか** を決めるクラスです。
自分自身はデータ処理も HTML 取得もしません。ただ順番を指示するだけです。

```bash
execute() が決めるフロー:
  1. _setup()          ← 「ブラウザを起動しなさい」   (※具体的な方法はサブクラスが決める)
  2. prepare()         ← 「ログインしてね」           (※任意)
  3. parse(url)        ← 「データを取ってきて」       (※開発者が実装する)
     └─ pipeline.process_item()  ← 「データを検品して保存して」
  4. finalize()        ← 「後片付けして」             (※任意)
  5. _teardown_resources() ← 「ブラウザを閉じて」
```

### デザインパターン: Template Method

これは **「Template Method パターン」** という有名な設計手法です。

**考え方:** 「処理の骨格（テンプレート）は親クラスで固定し、具体的な中身は子クラスに任せる」

```text
    ┌────────────────────────────────────────────┐
    │ BaseCrawler (親)                      　　  │
    │                                            │
    │  execute():          ← 処理: 順番を固定　　   │
    │    1. _setup()       ← 抽象: 子が実装        │
    │    2. prepare()      ← フック: 任意で上書き   │
    │    3. parse()        ← 抽象: 子が実装        │
    │    4. finalize()     ← フック: 任意で上書き   │
    │    5. _teardown()    ← 抽象: 子が実装        │
    └────────────────────────────────────────────┘
              ▲                   ▲
              │                   │
    ┌─────────┴──────┐   ┌────────┴──────┐
    │ StaticCrawler  │   │ DynamicCrawler│
    │                │   │               │
    │ _setup():      │   │ _setup():     │
    │  Requests起動　 │   │  Playwright   │
    │                │   │  ブラウザ起動   │
    │ _teardown():   │   │               │
    │  session.close │   │ _teardown():  │
    └────────────────┘   │  browser.close│
              ▲          └───────────────┘
              │                   ▲
    ┌─────────┴──────┐            │
    │ TablogScraper  │      (開発者が作る)
    │                │
    │ parse():       │
    │  サイト固有の    │
    │　スクレイピング　 │
    └────────────────┘
```

### BaseCrawlerコードの詳細解説

#### import 部分

```python
import abc
import logging
import time
from pathlib import Path
from typing import Generator, final

from src.framework.pipeline import ItemPipeline
```

ここで大事なのは:

- **`ItemPipeline` だけを import している**（`Schema` も `normalizer` も直接使わない）
- データ処理は完全に Pipeline に任せている

#### `__init_subclass__` — クラス定義時のガード {#_init_subclass_base}

```python
def __init_subclass__(cls, **kwargs):
    super().__init_subclass__(**kwargs)

    # フレームワーク内部のクラス（StaticCrawler 等）はチェック免除
    if cls.__module__.startswith("src.framework"):
        return

    # ユーザーコードで禁止メソッドを定義していたらエラー
    forbidden_methods = ["execute", "__init__"]

    for method in forbidden_methods:
        if method in cls.__dict__:
            raise TypeError(f"'{method}' のオーバーライドは禁止です")
```

**`cls.__module__`** は「そのクラスがどのファイルで定義されたか」を示す文字列です。

```python
# src/framework/static.py で定義された StaticCrawler の場合:
StaticCrawler.__module__  # => "src.framework.static"

# scripts/sites/tablog.py で定義された TablogScraper の場合:
TablogScraper.__module__  # => "scripts.sites.tablog"
```

- `"src.framework.static".startswith("src.framework")` → `True` → チェック免除
- `"scripts.sites.tablog".startswith("src.framework")` → `False` → チェック実行

**`cls.__dict__`** には、「そのクラス自身で定義したもの」だけが入ります。
親から継承したメソッドは入りません。

```python
class TablogScraper(StaticCrawler):
    def parse(self, url):    # ← これは cls.__dict__ に入る
        ...
    # execute は定義していない → cls.__dict__ に入らない → チェック通過
```

#### `__init__` — 初期化処理 {#initbase}

```python
def __init__(self):
    # クラス名付きロガー（並行実行時にどのクローラーか識別可能）
    self.logger = logging.getLogger(self.__class__.__name__)

    # output フォルダの準備
    base_dir = Path(__file__).resolve().parent.parent.parent
    self.local_output_dir = base_dir / "output"
    self.local_output_dir.mkdir(parents=True, exist_ok=True)

    # データ処理パイプラインを作成（Composition パターン）
    self.pipeline = ItemPipeline(
        output_dir=self.local_output_dir,
        crawler_name=self.__class__.__name__,
        extra_columns=self.EXTRA_COLUMNS,   # カスタムカラムを Pipeline に渡す
    )

    self.output_filepath = None
    self.observed_columns: list[str] = []   # データカタログ用
    self.item_count: int = 0                # 件数カウント
    self.extra_columns: list[str] = []      # EXTRA カラム
```

**`self.logger`:** `logging.getLogger(self.__class__.__name__)` でクラス名がログに出力されます。
複数のクローラーが並行実行されている場合、どのクローラーのログか一目で判別できます。

**`Path(__file__).resolve().parent.parent.parent`** は「このファイルの3階層上のフォルダ」です。

```python
Path(__file__)                   # => .../src/framework/base.py
  .resolve()                     # => 絶対パスに変換
  .parent                        # => .../src/framework/
  .parent                        # => .../src/
  .parent                        # => .../dso_system/      ← ここ！
```

**`self.__class__.__name__`** は「実際のクラス名」です。

```python
scraper = TablogScraper()
scraper.__class__.__name__   # => "TablogScraper" （"BaseCrawler" ではない！）
```

継承していても、実際のクラス名が使われます。
だから出力ファイル名は `20260213_TablogScraper.csv` になります。

**Composition パターン:**
ここで `self.pipeline = ItemPipeline(...)` と書いています。
BaseCrawler は ItemPipeline を**継承するのではなく、持っている（所有している）** だけです。
これを **Composition（合成）** と呼びます。

```python
X 継承 (is-a): "BaseCrawler は Pipeline である"    ← 意味がおかしい
O 合成 (has-a): "BaseCrawler は Pipeline を持っている" ← 自然！
```

#### `execute()` — メインの実行フロー

```python
@final
def execute(self, url: str):
    self.logger.info("開始: %s (待機間隔: %s秒)", self.__class__.__name__, self.DELAY)

    try:
        self._setup()              # 1. ブラウザ/セッション起動
        self.prepare()             # 2. 前処理（ログインなど）

        count = 0
        for item in self.parse(url):       # 3. メインループ（ジェネレータ）
            try:
                self.pipeline.process_item(item)   # Pipeline に検証・保存を委譲
                count += 1

                if self.DELAY > 0:
                    time.sleep(self.DELAY)  # サーバーへの負荷軽減

            except ValueError as ve:       # Schema 違反 → 即停止
                self.logger.error("致命的エラー (スキーマ違反): %s", ve)
                raise

            except Exception as item_error:   # その他のエラー → スキップして次へ
                self.logger.warning("アイテムをスキップしました (エラー: %s)", item_error)
                continue

        self.finalize()            # 4. 後処理
        self.logger.info("完了: %d 件処理しました", count)

    except Exception as e:
        self.logger.error("致命的エラー (システム): %s", e)
        raise

    finally:                       # !!! 必ず実行される（エラーが起きても）
        self.pipeline.close()      # CSV ファイルを閉じる
        self.output_filepath = self.pipeline.output_filepath
        self.observed_columns = self.pipeline.observed_columns   # カタログ用
        self.item_count = self.pipeline.item_count               # 件数
        self.extra_columns = self.pipeline.extra_columns         # EXTRA
        self._teardown_resources() # ブラウザ/セッションを閉じる
```

**`try-except-finally` の構造:**

```python
try:
    正常な処理フロー
except:
    エラー時の処理
finally:
    * 成功でもエラーでも必ず実行される *
```

`finally` ブロックは、エラーで途中停止しても**絶対に実行**されます。
ここにリソース解放（ファイルを閉じる、ブラウザを閉じる）を書くことで、
開きっぱなしのファイルやブラウザが残るのを防ぎます。

**エラーの処理（`CONTINUE_ON_ERROR` による制御）:**

- `ValueError` (Schema 違反) → **常に即停止。** 開発者のコードにバグがある。直すべき。
- `KeyboardInterrupt` (Ctrl+C) → **常に即停止。** ユーザーの意図的な停止。
- それ以外の `Exception` → `CONTINUE_ON_ERROR=True`（デフォルト）なら**スキップして次へ**、`False` なら即停止。
- `get_soup()` の通信エラー → `CONTINUE_ON_ERROR=True` なら `None` を返す、`False` なら例外を発生。
- スキップされたエラー件数は `self.error_count` に記録され、完了ログに表示されます。

#### 抽象メソッドとフックメソッド

```python
# ★ 必ず実装する (これが無いとインスタンス化できない)
@abc.abstractmethod
def parse(self, url):    ...
@abc.abstractmethod
def _setup(self):        ...
@abc.abstractmethod
def _teardown_resources(self):  ...

# ★ 任意で上書きする (デフォルトでは何もしない)
def prepare(self):   pass
def finalize(self):  pass
```

||種類|実装しないとどうなる？|例|
|---|---|---|---|
|`@abstractmethod`|抽象|エラーになる|`parse`, `_setup`|
|フック|任意|何も起きない (`pass`)|`prepare`, `finalize`|

---

## 8. Step 5: StaticCrawler / DynamicCrawler — 道具の違い

### StaticCrawler (`src/framework/static.py`)

**「静的サイト」** = JavaScript なしでHTML が完成しているサイト向け。
`requests` ライブラリ（軽量・高速）を使います。

```python
class StaticCrawler(BaseCrawler, abc.ABC):
    USER_AGENT = "Mozilla/5.0 ..."
    TIMEOUT = 30

    def __init__(self):
        super().__init__()          # ← BaseCrawler の __init__ を呼ぶ（Pipeline 作成など）
        self.session = None

    def _setup(self):               # BaseCrawler の抽象メソッドを実装
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": self.USER_AGENT})

    def _teardown_resources(self):  # BaseCrawler の抽象メソッドを実装
        if self.session:
            self.session.close()
        super()._teardown_resources()

    def get_soup(self, url):        # 便利メソッド（開発者向け）
        response = self.session.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return bs4.BeautifulSoup(response.text, "html.parser")
```

**`super().__init__()`** は「親クラス (`BaseCrawler`) の `__init__` を呼ぶ」という意味です。
これを忘れると、Pipeline の作成や output フォルダの準備が行われません。

**`Retry` と `HTTPAdapter`:**
ネットワークエラー時に自動で最大3回リトライする設定です。
`backoff_factor=1` は「1秒 → 2秒 → 4秒」と待ち時間を指数的に増やします。

**`get_soup()`** は開発者向けの便利メソッドです。
URL を渡すだけで、HTML を取得して BeautifulSoup オブジェクトに変換して返します。
`CONTINUE_ON_ERROR=True`（デフォルト）の場合、通信エラー（404、タイムアウト、接続拒否など）が発生すると例外を投げずに `None` を返します。これにより、1つのURLの取得失敗がクローラー全体を止めません。`CONTINUE_ON_ERROR=False` に設定すると従来どおり例外を発生させます。

> **DynamicCrawler にも `get_soup()` があります。** Playwright でページを取得して BeautifulSoup を返す便利メソッドで、同様のエラー耐性が適用されます。

### DynamicCrawler (`src/framework/dynamic.py`)

**「動的サイト」** = JavaScript で後からデータを読み込むサイト向け。
`Playwright`（ブラウザ自動操作）を使います。

構造は StaticCrawler と同じですが、使う道具が違います:

- `_setup()` → Playwright のブラウザを起動
- `_teardown_resources()` → ページ → コンテキスト → ブラウザ → Playwright の順に閉じる
- `get_soup(url)` → Playwright でページを取得し BeautifulSoup を返す（エラー耐性付き）
- 開発者は `self.page` を使って `self.page.goto(url)` のように直接操作することも可能

### StaticCrawler vs DynamicCrawler の選び方

|基準|StaticCrawler|DynamicCrawler|
|---|---|---|
|JavaScript が必要？|不要|必要|
|速度|高速|やや遅い|
|メモリ使用量|少ない|多い（ブラウザ起動）|
|使う道具|`requests` + `BeautifulSoup`|`Playwright`|
|例|企業一覧ページ|SPAサイト、無限スクロール|

---

## 9. 全体の処理フロー: 一連の流れを追う

実際に `TablogScraper` が実行される流れを、**時系列** で追ってみましょう。

```python
# scripts/debug.py から呼ばれると仮定
scraper = TablogScraper()
scraper.execute("https://tablog.jp/area/tokyo/")
```

### Phase 0: クラス定義時（`class TablogScraper(StaticCrawler):` の瞬間）

```python
TablogScraper を定義
  └── __init_subclass__ が自動実行
       ├── cls.__module__ = "scripts.sites.tablog"
       ├── "src.framework" で始まらない  → チェック実行
       ├── "execute" in cls.__dict__?    → False → OK
       └── "__init__" in cls.__dict__?   → False → OK
       ✅ 定義成功
```

### Phase 1: インスタンス生成（`TablogScraper()`）

```python
TablogScraper() が呼ばれる
  └── StaticCrawler.__init__()
       ├── super().__init__()  =  BaseCrawler.__init__()
       │    ├── output フォルダを作成
       │    ├── ItemPipeline を作成 (crawler_name="TablogScraper")
       │    └── output_filepath = None
       └── self.session = None
```

### Phase 2: 実行（`scraper.execute(url)`）

```python
execute(url) 開始
  │
  ├── 1. _setup()
  │    └── StaticCrawler._setup()
  │         └── requests.Session() を作成、User-Agent 設定、リトライ設定
  │
  ├── 2. prepare()
  │    └── (開発者が未実装なら何もしない = pass)
  │
  ├── 3. メインループ: for item in self.parse(url)
  │    │
  │    ├── parse() が yield  {"名称": "食べログカフェ", "TEL": "０３−１２３４−５６７８"}
  │    │     │
  │    │     ▼
  │    │   pipeline.process_item(item)
  │    │     ├── _validate_and_normalize()
  │    │     │    ├── "名称" in _ALLOWED_COLUMNS? → ✅
  │    │     │    │    normalize("名称", "食べログカフェ") → "食べログカフェ"  (ルールなし)
  │    │     │    └── "TEL" in _ALLOWED_COLUMNS?  → ✅
  │    │     │         normalize("TEL", "０３−１２３４−５６７８") → "03-1234-5678"
  │    │     └── _write_to_csv()
  │    │          └── (初回) ファイル作成: 20260213_TablogScraper.csv
  │    │          └── 1行書き込み: {"名称": "食べログカフェ", "TEL": "03-1234-5678"}
  │    │
  │    ├── time.sleep(DELAY)  ← サーバーに優しく
  │    │
  │    ├── parse() が yield  {"名称": "渋谷レストラン", "TEL": "03-5678-9012"}
  │    │     ▼
  │    │   pipeline.process_item(item)  →  2行目書き込み
  │    │   ...（繰り返し）
  │    │
  │    └── parse() が終了（yield がもうない）
  │
  ├── 4. finalize()
  │    └── (未実装なら何もしない)
  │
  └── finally:
       ├── pipeline.close()          ← CSV ファイルを閉じる
       ├── output_filepath = pipeline.output_filepath  ← パスを引き継ぐ
       └── _teardown_resources()     ← session.close()
```

### 結果

```bash
./output/20260213_TablogScraper.csv:

名称,TEL
食べログカフェ,03-1234-5678
渋谷レストラン,03-5678-9012
...
```

---

## 10. 設計原則: なぜこう分けたのか？

### 10.1 単一責任の原則 (SRP: Single Responsibility Principle)

**[クラスは、変更される理由が一つだけであるべきである。]**

|クラス/モジュール|責務|変更する理由|
|---|---|---|
|`Schema`|カラム名の定義|新しいカラムを追加したいとき|
|`normalizer.py`|データの正規化|新しい正規化ルールを追加したいとき|
|`ItemPipeline`|検証→正規化→CSV保存|保存形式を変えたいとき (CSV → JSON 等)|
|`BaseCrawler`|実行フローの制御|実行手順を変えたいとき|
|`StaticCrawler`|HTTP セッション管理|通信方法を変えたいとき|

> もし全部が1ファイルだったら？
> → 「正規化ルールを追加する」だけなのに、500 行のファイルを開いて、
> 関係ない部分を壊さないか心配しながら変更するはめになります。

### 10.2 Convention over Configuration（設定より規約）

**[ 何も設定しなくても動く。特別なことをしたいときだけ追加で設定する ]**

```bash
# これだけで動く（環境変数、設定ファイル、DB 接続、一切不要）
python scripts/debug.py --site tablog --url "https://..."
```

環境変数が必要なのは Docker Compose（インフラ層）だけ。
アプリコード（Python）が要求する環境変数は **0個** です。

### 10.3 疎結合 — 知らないことは強さ

各モジュールが「知っていること」を整理すると:

```python
normalizer.py が知っていること:
  ✅ Schema のカラム名
  ❌ Pipeline の存在を知らない
  ❌ BaseCrawler の存在を知らない
  ❌ CSV の保存先を知らない

ItemPipeline が知っていること:
  ✅ Schema のカラム名
  ✅ normalizer の normalize() 関数
  ❌ BaseCrawler の存在を知らない
  ❌ どのサイトをスクレイピングしているか知らない

BaseCrawler が知っていること:
  ✅ ItemPipeline
  ❌ Schema を直接使わない
  ❌ normalizer を直接使わない
  ❌ requests も Playwright も知らない
```

**「知らない」ということは、相手が変わっても自分は変更不要** ということです。
例えば `normalizer.py` のルールを大幅に書き換えても、`BaseCrawler` は**1行も変更不要**です。

---

## 11. 実際にスクレイパーを作る手順

### Step 1: ファイルを作る

```python
# scripts/sites/tablog.py
from src.framework import StaticCrawler
from src.const.schema import Schema
```

### Step 2: クラスを定義する

```python
class TablogScraper(StaticCrawler):
    DELAY = 2.0     # リクエスト間隔（秒）。サーバーに優しく。
```

サイト固有のカラムが必要な場合は `EXTRA_COLUMNS` を宣言します:

```python
class TablogScraper(StaticCrawler):
    DELAY = 2.0
    EXTRA_COLUMNS = ["駐車場", "営業時間"]   # ← Schema にない独自カラムを宣言
```

> **重要:** `EXTRA_COLUMNS` を書かなければ、Schema に定義されたカラムだけが許可されます（従来通り）。
> 宣言していないカスタムカラムを yield に含めると `ValueError` で即停止します。
> 「うっかり」ではなく「意図的に」カスタムカラムを使うための仕組みです。

### Step 3: `parse()` を実装する

```python
    def parse(self, url):
        soup = self.get_soup(url)              # StaticCrawler の便利メソッド

        for shop in soup.select("div.shop"):   # HTML からデータを抽出
            yield {
                Schema.NAME: shop.select_one("h2").text.strip(),
                Schema.TEL:  shop.select_one("span.tel").text,
                Schema.ADDR: shop.select_one("p.address").text,
                Schema.URL:  url,
                # EXTRA_COLUMNS で宣言したカラムも使える
                "駐車場":    shop.select_one("span.parking").text if shop.select_one("span.parking") else "",
                "営業時間":  shop.select_one("span.hours").text if shop.select_one("span.hours") else "",
            }
```

### Step 4: 実行する

```python
# scripts/debug.py
from scripts.sites.tablog import TablogScraper

scraper = TablogScraper()
scraper.execute("https://tablog.jp/area/tokyo/")
print(f"完了！ファイル: {scraper.output_filepath}")
```

**これだけです。** CSV 保存、電話番号の正規化、エラー処理は全て自動です。

### もしログインが必要なサイトなら？

```python
class LoginRequiredScraper(StaticCrawler):
    DELAY = 3.0

    def prepare(self):                         # ← フックメソッドを上書き
        self.session.post("https://example.com/login", data={
            "username": "user",
            "password": "pass",
        })

    def parse(self, url):
        soup = self.get_soup(url)              # ← ログイン済みセッションで取得
        ...
```

---

## 12. よくある疑問 Q&A

### Q1: `@abc.abstractmethod` と `@final` は仕組みが違うの？

||`@abstractmethod`|`@final`|
|---|---|---|
|意味|子クラスで必ず実装して|子クラスで上書きしないで|
|強制力|**あり** (実装しないとインスタンス化できない)|**弱い** (型チェッカーが警告するだけ)|
|検査タイミング|インスタンス生成時|mypy 等の静的解析時|

`__init_subclass__` は `@final` より**強い**保護です。
クラス定義時に `TypeError` を出して、実行そのものを止めます。

### Q2: なぜ `print()` ではなく `logging` を使うの？

```python
# print() だと…
print("処理開始")    # テスト時に消したい → コメントアウト? 面倒…
print("エラー！")    # 他のメッセージに埋もれる

# logging だと…
logger.info("処理開始")      # ← logging.basicConfig(level=logging.WARNING) で消せる
logger.error("エラー！")     # ← level を変えても、ERROR は常に表示される
```

### Q3: Schema にないカラムを使いたい場合は？

2つの方法があります:

**A. 全サイト共通のカラムを追加する場合:**

`src/const/schema.py` に1行足すだけです:

```python
class Schema:
    ...
    FAX = "FAX"    # ← 追加
```

他のファイルは**一切変更不要**。`_ALLOWED_COLUMNS` はモジュール読み込み時に自動更新されます。

**B. 特定のサイトだけの固有カラムを使う場合:**

クローラーの `EXTRA_COLUMNS` で宣言します:

```python
class TablogScraper(StaticCrawler):
    EXTRA_COLUMNS = ["駐車場", "営業時間"]   # ← このサイトだけのカラム

    def parse(self, url):
        yield {
            Schema.NAME: "食べログカフェ",
            "駐車場":    "あり",              # ← 宣言済みなのでOK
            "営業時間":  "11:00〜22:00",      # ← 宣言済みなのでOK
        }
```

> **`EXTRA_COLUMNS` を宣言しないと** カスタムカラムは使えません。
> 「うっかり」ではなく「意図的に」使うためのガードレールです。

両方の方法とも、`base.py` や `pipeline.py` の変更は不要です。

### Q4: `_` で始まるメソッドと始まらないメソッドの違いは？

```python
class ItemPipeline:
    def process_item(self, item):          # ← 外から呼んで OK
        clean = self._validate_and_normalize(item)   # ← 内部用
        self._write_to_csv(clean)                     # ← 内部用
```

- `_` なし → **公開 API**: 他のクラスから呼ぶことを想定
- `_` あり → **内部用**: このクラスの中だけで使う
- `__` 2つ → **名前修飾 (name mangling)**: さらに強い「触るな」サイン

Python は**アクセス制御を強制しません**（Java の `private` とは違う）。
あくまで「この変数/メソッドは内部用ですよ」という**開発者間のルール**です。

### Q5: `super()` って何をしているの？

**「親クラスのメソッドを呼ぶ」** ための仕組みです。

```python
class StaticCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()      # ← BaseCrawler.__init__() を呼ぶ
        self.session = None     # ← StaticCrawler 独自の初期化
```

もし `super().__init__()` を書き忘れると:

- `self.pipeline` が作られない → `execute()` で `AttributeError`
- `self.local_output_dir` が作られない
- 全て壊れる

**教訓: 親クラスの `__init__` を `super()` で必ず呼ぶ。**

### Q6: `yield` と `return` の違いを改めて教えて

```python
# return: 値を1つ返して関数が終了する
def get_all():
    return [1, 2, 3, 4, 5]    # メモリに全部載せてから返す

# yield: 値を1つずつ返す。関数は一時停止して、次に呼ばれたら再開する
def get_one_by_one():
    yield 1    # ← 1 を返して一時停止
    yield 2    # ← 次の for ループで 2 を返して一時停止
    yield 3    # ...
```

10万件のデータがある場合:

- `return`: 10万件分の辞書をメモリに載せてから処理 → メモリ大量消費
- `yield`: 1件ずつメモリに載せて使い終わったら捨てる → メモリ最小限

スクレイピングでは**件数が事前にわからない**ことが多いので、`yield` が最適です。

---

## 13. 全自動データカタログ機能

### 13.1 この機能は何をするのか？

クローラーが実行されるたびに、「そのサイトから**実際に取得できたデータ項目（カラム）**」をシステムが自動で検知し、以下の3ヶ所に同期します。

| 同期先 | 用途 | ファイル/テーブル |
|--------|------|-------------------|
| PostgreSQL | SSOT マスターデータ | `data_catalog` テーブル |
| CSV | 営業配布用マトリクス | `output/data_catalog.csv` |
| Prefect Dashboard | 可視化 | Markdown Artifact |

**Schema カラムと EXTRA カラムは分離して管理されます。** Schema カラムは ◯✕ マトリクスで表示、EXTRA カラムはカンマ区切りで 1 列にまとめて表示します。

### 13.2 データフロー

```text
parse() → yield dict
    ↓
Pipeline.process_item()
    ├→ バリデーション + 正規化
    ├→ CSV 保存
    ├→ _observed_columns.update(keys)   ← カラム記録
    └→ _item_count += 1                 ← 件数カウント
    ↓
execute() 完了 (finally)
    ↓
scraper.observed_columns → ["名称", "住所", "TEL", ...]
scraper.extra_columns    → ["備考"]
scraper.item_count       → 5
    ↓
run_flow.py: sync_data_catalog_task()
    ├→ ① PostgreSQL UPSERT (columns + extra_columns を分離保存)
    ├→ ② CSV マトリクス出力 (Schema ◯✕ + EXTRA 列)
    └→ ③ Prefect create_markdown_artifact
```

### 13.3 実装の仕組み

#### Pipeline（カラム収集 + 件数カウント）

```python
# pipeline.py
class ItemPipeline:
    def __init__(self, ...):
        self._observed_columns: set[str] = set()
        self._item_count: int = 0
        self._extra_columns = list(extra_columns or [])

    def process_item(self, item: dict) -> dict:
        clean_item = self._validate_and_normalize(item)
        self._observed_columns.update(clean_item.keys())  # カラム記録
        self._item_count += 1                              # 件数カウント
        self._write_to_csv(clean_item)
        return clean_item

    @property
    def observed_columns(self) -> list[str]:
        return sorted(self._observed_columns)

    @property
    def item_count(self) -> int:
        return self._item_count

    @property
    def extra_columns(self) -> list[str]:
        return list(self._extra_columns)
```

#### BaseCrawler（外部への公開）

```python
# base.py (execute() の finally ブロック)
finally:
    self.pipeline.close()
    self.output_filepath = self.pipeline.output_filepath
    self.observed_columns = self.pipeline.observed_columns  # Schema + EXTRA 混在
    self.item_count = self.pipeline.item_count              # 件数
    self.extra_columns = self.pipeline.extra_columns        # EXTRA のみ
    self._teardown_resources()
```

- `observed_columns` は全カラム（Schema + EXTRA 混在）
- `extra_columns` は EXTRA カラムのみ
- `run_flow.py` 側で分類して catalog に渡す

#### catalog.py（3ヶ所同期）

`src/utils/catalog.py` に同期ロジックを集約しています（単一責任原則）。

- `upsert_to_db()` — PostgreSQL への UPSERT（columns + extra_columns を分離保存）
- `export_matrix_csv()` — CSV マトリクス出力（Schema ◯✕ + EXTRA 列）
- `generate_markdown_table()` — Markdown テーブル文字列生成（同上）

### 13.4 DB テーブル設計

```sql
CREATE TABLE IF NOT EXISTS data_catalog (
    site_name      TEXT PRIMARY KEY,
    columns        JSONB NOT NULL DEFAULT '[]'::JSONB,   -- Schema カラム
    extra_columns  JSONB NOT NULL DEFAULT '[]'::JSONB,   -- EXTRA カラム
    item_count     INTEGER NOT NULL DEFAULT 0,           -- 取得件数
    last_run_at    TIMESTAMPTZ DEFAULT NOW(),             -- 最終実行日時
    updated_at     TIMESTAMPTZ DEFAULT NOW()              -- 最終更新日時
);
```

- JSONB を使用しているため、カラムが増えても ALTER TABLE は不要
- columns と extra_columns を分離して保存
- テーブルは `bin/init_db.py` で**1回だけ**作成（ランタイムで DDL を実行しない）
- item_count=0 でクローラー故障を検知可能

### 13.5 CSV 出力イメージ

```text
サイト名, 名称, 名称_カナ, 住所, TEL, ..., EXTRA カラム, 取得件数, 最終実行日時
alarmbox,  ◯,    ◯,       ◯,   ✕,  ...,  -,           150,     2026-02-26...
dummy,     ◯,    ◯,       ◯,   ◯,  ...,  備考,          5,      2026-02-26...
```

- エンコーディング: `utf-8-sig`（BOM付き UTF-8）— Windows Excel で文字化けしない
- 出力先: `output/data_catalog.csv`
- 書き込み: アトミック（一時ファイル → `os.replace`）
- Schema カラムは ◯✕ マトリクス、EXTRA カラムはカンマ区切りで 1 列

### 13.6 環境変数

| 変数名 | 説明 | デフォルト値 |
|--------|------|-------------|
| `dso_system_DB_URL` | PostgreSQL 接続 URL | `postgresql://dso_system:dso_system@database:5432/dso_system` |
