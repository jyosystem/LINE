#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
line_url_finder.py
------------------
企業名のCSVを入力として受け取り、各企業のLINE公式アカウントURL候補を
収集してCSVを出力するCLIツール。

使い方:
    # 1. 環境変数を設定
    export GOOGLE_API_KEY="your_google_api_key"
    export GOOGLE_CSE_ID="your_custom_search_engine_id"

    # 2. 実行
    python line_url_finder.py --input companies.csv --output result.csv

    # オプション付き
    python line_url_finder.py \
        --input companies.csv \
        --output result.csv \
        --max-results 5 \
        --sleep 1.0 \
        --timeout 10

入力CSV例:
    company_name,company_url
    株式会社〇〇,https://example.com
    株式会社△△,

出力CSV例:
    company_name,company_url,line_url_candidates,confidence,source
    株式会社〇〇,https://example.com,https://lin.ee/xxxx,high,official_site
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

# ------------------------------------------------------------------
# 定数
# ------------------------------------------------------------------
GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"

# LINE 公式アカウントとして取りうるURLパターン
LINE_URL_PATTERNS = [
    re.compile(r"https?://lin\.ee/[A-Za-z0-9_\-]+", re.IGNORECASE),
    re.compile(r"https?://page\.line\.me/[A-Za-z0-9_\-?=&%./]+", re.IGNORECASE),
    re.compile(r"https?://line\.me/R/ti/p/[A-Za-z0-9_\-%@]+", re.IGNORECASE),
    re.compile(r"https?://line\.me/ti/p/[A-Za-z0-9_\-%@]+", re.IGNORECASE),
    re.compile(r"https?://liff\.line\.me/[A-Za-z0-9_\-]+", re.IGNORECASE),
]

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

# ------------------------------------------------------------------
# ロガー
# ------------------------------------------------------------------
logger = logging.getLogger("line_url_finder")


def setup_logger(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ------------------------------------------------------------------
# データ構造
# ------------------------------------------------------------------
@dataclass
class LineLookupResult:
    company_name: str
    company_url: Optional[str]
    line_url_candidates: List[str] = field(default_factory=list)
    confidence: str = "low"          # high / medium / low
    source: str = ""                 # google / official_site / google+official_site / ""

    def to_row(self) -> dict:
        return {
            "company_name": self.company_name,
            "company_url": self.company_url or "",
            "line_url_candidates": ",".join(self.line_url_candidates),
            "confidence": self.confidence,
            "source": self.source,
        }


# ------------------------------------------------------------------
# ユーティリティ
# ------------------------------------------------------------------
def extract_line_urls(text: str) -> Set[str]:
    """テキストから LINE 関連 URL を抽出する。"""
    if not text:
        return set()
    urls: Set[str] = set()
    for pat in LINE_URL_PATTERNS:
        for m in pat.findall(text):
            # 末尾の余計な文字を取り除く
            cleaned = m.rstrip(".,)\"'>]}")
            urls.add(cleaned)
    return urls


def normalize_url(url: str) -> str:
    """URL末尾のスラッシュ等を整形。"""
    return url.strip().rstrip("/")
def is_valid_url(url: str) -> bool:

    """URLの基本的な妥当性をチェックする。"""
    if not url:
        return False
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)

# ------------------------------------------------------------------
# Google Custom Search
# ------------------------------------------------------------------
def google_custom_search(
    query: str,
    api_key: str,
    cse_id: str,
    num: int = 10,
    timeout: int = 10,
) -> List[dict]:
    """Google Custom Search API を呼んで結果(items)を返す。"""
    params = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "num": min(max(num, 1), 10),  # CSE 1リクエストは最大10件
        "hl": "ja",
        "lr": "lang_ja",
    }
    try:
        resp = requests.get(GOOGLE_CSE_ENDPOINT, params=params, timeout=timeout)
    except requests.RequestException as e:
        logger.error("Google CSE リクエスト失敗: %s", e)
        return []

    if resp.status_code != 200:
        logger.error(
            "Google CSE が %s を返しました: %s",
            resp.status_code, resp.text[:200],
        )
        return []

    try:
        data = resp.json()
    except ValueError:
        logger.error("Google CSE のJSON解析に失敗")
        return []

    return data.get("items", []) or []


def search_line_urls_via_google(
    company_name: str,
    api_key: str,
    cse_id: str,
    max_results: int = 10,
    timeout: int = 10,
) -> Set[str]:
    """企業名からGoogle検索でLINE URL候補を集める。"""
    queries = [
        f'"{company_name}" LINE 公式アカウント',
        f'"{company_name}" lin.ee OR line.me',
    ]
    urls: Set[str] = set()
    for q in queries:
        items = google_custom_search(q, api_key, cse_id, num=max_results, timeout=timeout)
        for item in items:
            link = item.get("link", "") or ""
            snippet = item.get("snippet", "") or ""
            title = item.get("title", "") or ""

            # 直接ヒットしたURL
            if any(p.match(link) for p in LINE_URL_PATTERNS):
                urls.add(normalize_url(link))

            # スニペット/タイトル内のURL
            urls.update(normalize_url(u) for u in extract_line_urls(snippet))
            urls.update(normalize_url(u) for u in extract_line_urls(title))
    return urls


# ------------------------------------------------------------------
# 公式サイトのスクレイピング
# ------------------------------------------------------------------
def fetch_official_site_html(url: str, timeout: int = 10) -> Optional[str]:
    """公式サイトのHTMLを取得する。失敗時 None。"""
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": DEFAULT_USER_AGENT},
            allow_redirects=True,
        )
    except requests.RequestException as e:
        logger.warning("公式サイト取得失敗 %s: %s", url, e)
        return None

    if resp.status_code != 200:
        logger.warning("公式サイト %s が %s を返却", url, resp.status_code)
        return None

    # 文字化け対策
    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding
    return resp.text


def search_line_urls_via_official_site(
    company_url: Optional[str],
    timeout: int = 10,
) -> Set[str]:
    """公式サイト本文中のLINE URL候補を抽出する。"""
    if not company_url:
        return set()
    html = fetch_official_site_html(company_url, timeout=timeout)
    if not html:
        return set()
    urls = extract_line_urls(html)
    return {normalize_url(u) for u in urls}


# ------------------------------------------------------------------
# 信頼度判定
# ------------------------------------------------------------------
def decide_confidence(
    official_urls: Set[str],
    google_urls: Set[str],
) -> Tuple[str, str]:
    """
    confidence と source を判定する。

    - 公式サイトに直接LINE URLがある -> high / official_site (Googleにもあれば併記)
    - 公式にはなく、Googleで複数ヒット -> medium / google
    - Googleで1件のみ -> low / google
    - いずれもヒットなし -> low / ""
    """
    has_official = bool(official_urls)
    google_only = google_urls - official_urls

    if has_official and google_only:
        return "high", "google+official_site"
    if has_official:
        return "high", "official_site"
    if len(google_urls) >= 2:
        return "medium", "google"
    if len(google_urls) == 1:
        return "low", "google"
    return "low", ""


# ------------------------------------------------------------------
# メイン処理
# ------------------------------------------------------------------
def process_company(
    company_name: str,
    company_url: Optional[str],
    api_key: str,
    cse_id: str,
    max_results: int,
    timeout: int,
) -> LineLookupResult:
    """1社分の処理。"""
    result = LineLookupResult(company_name=company_name, company_url=company_url)

    # 1) 公式サイトの調査
    try:
        official_urls = search_line_urls_via_official_site(company_url, timeout=timeout)
    except Exception as e:  # 想定外
        logger.exception("公式サイト解析エラー (%s): %s", company_name, e)
        official_urls = set()

    # 2) Google検索
    try:
        google_urls = search_line_urls_via_google(
            company_name=company_name,
            api_key=api_key,
            cse_id=cse_id,
            max_results=max_results,
            timeout=timeout,
        )
    except Exception as e:
        logger.exception("Google検索エラー (%s): %s", company_name, e)
        google_urls = set()

    # マージ&順序付け（公式優先 → Google）
    merged: List[str] = []
    seen: Set[str] = set()
    for u in list(official_urls) + list(google_urls):
        if u not in seen:
            merged.append(u)
            seen.add(u)

    result.line_url_candidates = merged
    confidence, source = decide_confidence(official_urls, google_urls)
    result.confidence = confidence
    result.source = source
    return result


def load_input_csv(path: str) -> pd.DataFrame:
    """入力CSVを読み込む。"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"入力CSVが見つかりません: {path}")
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception as e:
        raise RuntimeError(f"CSV読み込み失敗: {e}") from e

    if "company_name" not in df.columns:
        raise ValueError("入力CSVに 'company_name' カラムが必要です。")

    if "company_url" not in df.columns:
        df["company_url"] = ""

    df["company_name"] = df["company_name"].astype(str).str.strip()
    df["company_url"] = df["company_url"].astype(str).str.strip()
    df = df[df["company_name"] != ""].reset_index(drop=True)
    return df


def save_output_csv(rows: List[dict], path: str) -> None:
    out_df = pd.DataFrame(
        rows,
        columns=[
            "company_name",
            "company_url",
            "line_url_candidates",
            "confidence",
            "source",
        ],
    )
    try:
        out_df.to_csv(path, index=False, encoding="utf-8-sig")
    except Exception as e:
        raise RuntimeError(f"CSV書き込み失敗: {e}") from e


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="企業名CSVからLINE公式アカウントURL候補を抽出してCSVを出力します。",
    )
    parser.add_argument("--input", "-i", required=True, help="入力CSVファイルパス")
    parser.add_argument("--output", "-o", required=True, help="出力CSVファイルパス")
    parser.add_argument("--max-results", type=int, default=10,
                        help="Google CSEで取得する1クエリあたりの最大件数 (1-10, default=10)")
    parser.add_argument("--sleep", type=float, default=1.0,
                        help="各企業処理の合間に入れる待機秒数 (default=1.0)")
    parser.add_argument("--timeout", type=int, default=10,
                        help="HTTPリクエストのタイムアウト秒数 (default=10)")
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログを表示")
    parser.add_argument("--api-key", default=os.getenv("GOOGLE_API_KEY", ""),
                        help="Google API キー (環境変数 GOOGLE_API_KEY でも可)")
    parser.add_argument("--cse-id", default=os.getenv("GOOGLE_CSE_ID", ""),
                        help="Google Custom Search Engine ID (環境変数 GOOGLE_CSE_ID でも可)")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    setup_logger(args.verbose)

    if not args.api_key or not args.cse_id:
        logger.error(
            "GOOGLE_API_KEY と GOOGLE_CSE_ID を環境変数または引数で指定してください。"
        )
        return 2

    try:
        df = load_input_csv(args.input)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        logger.error("入力CSVエラー: %s", e)
        return 2

    logger.info("対象企業数: %d", len(df))

    rows: List[dict] = []
    for idx, row in df.iterrows():
        company_name = row["company_name"]
        company_url = row.get("company_url") or None
        logger.info("[%d/%d] 処理中: %s", idx + 1, len(df), company_name)

        try:
            result = process_company(
                company_name=company_name,
                company_url=company_url,
                api_key=args.api_key,
                cse_id=args.cse_id,
                max_results=args.max_results,
                timeout=args.timeout,
            )
        except Exception as e:
            logger.exception("企業処理失敗 (%s): %s", company_name, e)
            result = LineLookupResult(
                company_name=company_name,
                company_url=company_url,
            )

        logger.debug(
            "  -> %d 件の候補 (confidence=%s, source=%s)",
            len(result.line_url_candidates),
            result.confidence,
            result.source,
        )
        rows.append(result.to_row())

        # レート制限対策
        if args.sleep > 0 and idx < len(df) - 1:
            time.sleep(args.sleep)

    try:
        save_output_csv(rows, args.output)
    except RuntimeError as e:
        logger.error("出力エラー: %s", e)
        return 2

    logger.info("完了: %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
