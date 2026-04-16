"""
AWS S3 へのアップロードユーティリティ。

スクレイピング結果 (CSV) を AWS S3 バケットの指定パスにアップロードします。
認証情報は .env ファイル（環境変数 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`）
によって自動的に boto3 側で読み込まれます。
"""

import os
import boto3
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path

from prefect.states import State, StateType
from prefect.logging import get_run_logger

logger = logging.getLogger(__name__)

def upload_to_s3(local_filepath: str, site_name_ja: str) -> str | State:
    """
    指定されたローカルファイルを S3 バケットにアップロードする。

    設定値 `S3_BUCKET_NAME` が存在しない、または S3 の認証情報が不足している場合は
    警告を出して処理をスキップ（Noneを返却）します。

    Args:
        local_filepath (str): アップロード元のローカルファイルパス
        site_name_ja (str): 格納先ディレクトリ名となるサイトの日本語名

    Returns:
        str | None: アップロード成功時は S3 URI (s3://bucket/key)、失敗またはスキップ時は None
    """
    bucket_name = os.environ.get("S3_BUCKET_NAME", "")
    if not bucket_name:
        logger.debug("S3_BUCKET_NAME が設定されていないため、S3アップロードをスキップします。")
        return State(type=StateType.COMPLETED, name="Skipped", message="S3_BUCKET_NAME が未設定です。")

    if not os.path.exists(local_filepath):
        logger.error("アップロード対象のファイルが存在しません: %s", local_filepath)
        return None

    filename = Path(local_filepath).name
    # 保存ルール: crawler_data/<site_name_ja>/YYYYMMDD_Sitename.csv
    s3_key = f"crawler_data/{site_name_ja}/{filename}"

    try:
        # boto3 クライアントの作成 (環境変数から認証情報が自動ロードされる)
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-1")
        )

        logger.info("S3へファイルアップロードを開始します... [Bucket: %s, Key: %s]", bucket_name, s3_key)
        
        s3_client.upload_file(local_filepath, bucket_name, s3_key)
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        logger.info("S3へのアップロードが完了しました: %s", s3_uri)
        return s3_uri
        
    except NoCredentialsError:
        logger.error("AWS の認証情報が見つかりません。.env ファイルに AWS_ACCESS_KEY_ID などを設定してください。")
        return None
    except ClientError as e:
        logger.error("S3アップロード中にエラーが発生しました: %s", e)
        return None
    except Exception as e:
        logger.error("予期せぬエラーが発生しました: %s", e)
        return None
