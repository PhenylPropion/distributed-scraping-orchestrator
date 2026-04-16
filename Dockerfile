# イメージ: Python 3.13-slim : Debianベース最小限パッケージ
# 本番環境でサイズを5倍縮小、セキュリティ面でパッケージを減らす
# ライブラリ非対応エラーが出たら 3.12-slim にする
FROM python:3.13-slim

# 環境変数の設定
# Pythonの標準出力と標準エラーをバッファリングせずにリアルタイムで出力する \
# UV_SYSTEM_PYTHON=1: 仮想環境を作らずシステムPythonにインストールする
ENV PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1

# mkdir -p /app && cd /app と同義
WORKDIR /app

# 0. システムパッケージのインストール (git 等)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# 1. uv のバイナリだけを公式イメージからコピー (最速)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 2. 依存定義ファイルのコピー
# まず pyproject.toml だけをコピー -> キャッシュが効くから
# uv.lock は存在しない場合エラーにならないようアスタリスクを付けるか、なければ無視される
COPY pyproject.toml uv.lock* ./

# 3. 依存パッケージのインストール
# --system: システムPythonに入れる
# pyproject.toml の依存関係をuv pip install でライブラリを入れる
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r pyproject.toml

# 4. Playwrightのために、Chromiumブラウザとその依存関係(--with-deps)をインストール
RUN playwright install --with-deps chromium

# 5. ソースコードパスの設定
# /app の中にあるフォルダなどをモジュールとして読み込み
ENV PYTHONPATH=/app

# コンテナ起動時に実行するコマンド
# 常駐するために、何もしないコマンド(tail -f /dev/null)でコンテナを無理やり生かす。
CMD ["tail", "-f", "/dev/null"]