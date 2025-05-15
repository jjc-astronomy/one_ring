import argparse
import logging
import os
import sys
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from uuid_utils import UUIDUtility

# Assumes a MySQL unique constraint on classified_candidates:
#   UNIQUE(fold_candidate_id, search_candidate_id, candidate_viewer_id)
# This lets us use INSERT ... ON DUPLICATE KEY UPDATE for idempotent upserts.


def parse_args():
    parser = argparse.ArgumentParser(
        description="Bulk-upload user labels into the classified_candidates table"
    )
    parser.add_argument(
        "csv_files", nargs='+', help="One or more CSVs with user labels"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )
    return parser.parse_args()


class LabelUploader:
    def __init__(self, env_path: Path, verbose: bool = False):
        # Load DB credentials from .env
        load_dotenv(dotenv_path=env_path)
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(format="%(message)s", level=level)
        self.logger = logging.getLogger("LabelUploader")

        connection_url = URL.create(
            "mysql+mysqlconnector",
            username=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "3306")),
            database=os.getenv("DB_NAME")
        )
        self.engine = create_engine(connection_url, echo=False)
        self.metadata = MetaData()
        self.metadata.reflect(
            bind=self.engine,
            only=[
                'candidate_viewer',
                'classification_category',
                'fold_candidate',
                'classified_candidates'
            ]
        )
        self.tbl_viewer = self.metadata.tables['candidate_viewer']
        self.tbl_category = self.metadata.tables['classification_category']
        self.tbl_fold = self.metadata.tables['fold_candidate']
        self.tbl_classified = self.metadata.tables['classified_candidates']

    def _ensure_viewers(self, usernames):
        # Bulk ensure usernames exist, return map username->id
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(self.tbl_viewer.c.id, self.tbl_viewer.c.username)
                .where(self.tbl_viewer.c.username.in_(usernames))
            ).fetchall()
            existing = {u: i for i, u in rows}
            missing = set(usernames) - set(existing)
            if missing:
                self.logger.debug(f"Inserting missing viewers: {missing}")
                conn.execute(
                    self.tbl_viewer.insert(),
                    [{'username': u} for u in missing]
                )
                conn.commit()
                rows = conn.execute(
                    select(self.tbl_viewer.c.id, self.tbl_viewer.c.username)
                    .where(self.tbl_viewer.c.username.in_(usernames))
                ).fetchall()
                existing = {u: i for i, u in rows}
        return existing

    def _ensure_categories(self, categories):
        # Bulk ensure categories exist, return map category->id
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(self.tbl_category.c.id, self.tbl_category.c.category)
                .where(self.tbl_category.c.category.in_(categories))
            ).fetchall()
            existing = {c: i for i, c in rows}
            missing = set(categories) - set(existing)
            if missing:
                self.logger.debug(f"Inserting missing categories: {missing}")
                conn.execute(
                    self.tbl_category.insert(),
                    [{'category': c} for c in missing]
                )
                conn.commit()
                rows = conn.execute(
                    select(self.tbl_category.c.id, self.tbl_category.c.category)
                    .where(self.tbl_category.c.category.in_(categories))
                ).fetchall()
                existing = {c: i for i, c in rows}
        return existing

    def _map_search_ids(self, fold_uuid_bins):
        # Map fold_candidate_id_bin -> search_candidate_id from fold_candidate
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    self.tbl_fold.c.id,
                    self.tbl_fold.c.search_candidate_id
                ).where(
                    self.tbl_fold.c.id.in_(fold_uuid_bins)
                )
            ).fetchall()
            return {fid: sid for fid, sid in rows}

    def process_files(self, paths):
        # Read CSVs, infer user_id if missing, drop UNCATEGORIZED, and upsert labels
        dfs = []
        for p in paths:
            self.logger.debug(f"Reading {p}")
            df_i = pd.read_csv(p)
            if 'username' not in df_i.columns:
                stem = Path(p).stem
                parts = stem.split('_')
                if len(parts) >= 2 and parts[-1] == 'full':
                    username = parts[-2]
                    self.logger.debug(
                        f"No username; using '{username}' for all rows of {p.name}"
                    )
                    df_i['username'] = username
                else:
                    self.logger.error(
                        f"Filename '{p.name}' lacks '<username>_full' pattern; cannot infer username."
                    )
                    sys.exit(1)
            dfs.append(df_i)
        df = pd.concat(dfs, ignore_index=True)

        # Drop rows with classification 'UNCAT'
        mask_uncat = df['classification'] == 'UNCAT'
        dropped = mask_uncat.sum()
        if dropped > 0:
            self.logger.warning(f"Dropping {dropped} UNCATEGORIZED rows")
            df = df[~mask_uncat]

        # 1) Ensure viewers; map to viewer_id
        viewers = self._ensure_viewers(df['username'].unique())
        df['viewer_id'] = df['username'].map(viewers)

        # 2) Ensure categories; map to category_id
        cats = self._ensure_categories(df['classification'].unique())
        df['category_id'] = df['classification'].map(cats)

        # 3) Convert fold_candidate_id_bin to binary and map to search IDs
        df['fold_bin'] = df['fold_candidate_id_bin'].apply(
            UUIDUtility.convert_uuid_string_to_binary
        )
        search_map = self._map_search_ids(df['fold_bin'].unique().tolist())
        df['search_id'] = df['fold_bin'].map(search_map)

        # 4) Upsert into classified_candidates using ON DUPLICATE KEY
        records = [
            {
                'classification_id': rec.category_id,
                'fold_candidate_id': rec.fold_bin,
                'search_candidate_id': rec.search_id,
                'candidate_viewer_id': rec.viewer_id
            }
            for rec in df.itertuples(index=False)
        ]
        stmt = mysql_insert(self.tbl_classified).values(records)
        stmt = stmt.on_duplicate_key_update(
            # If exists with different classification, update it;
            # if identical, does nothing.
            classification_id=stmt.inserted.classification_id
        )
        with self.engine.connect() as conn:
            self.logger.debug(f"Upserting {len(records)} labels into classified_candidates")
            conn.execute(stmt)
            conn.commit()
        self.logger.info("Classification Upload complete.")


def main():
    args = parse_args()
    uploader = LabelUploader(env_path=Path('.compactdb.env'), verbose=args.verbose)
    uploader.process_files([Path(f) for f in args.csv_files])


if __name__ == '__main__':
    main()
