from __future__ import annotations

import argparse
from pathlib import Path

import rerun as rr

from lerobot_preview.gcp_support import GCPLeRobot


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", type=str, help="GCP bucket name")
    parser.add_argument("prefix", type=Path, help="Path to directory containing LeRobot Dataset")
    parser.add_argument("episode", type=str, help="Episode name")
    parser.add_argument("--project", default=None, help="GCP project name")

    args = parser.parse_args()

    bucket = args.bucket
    prefix = args.prefix
    cloud_loader = GCPLeRobot(bucket, prefix, args.project)
    cloud_loader.get_metadata()
    cloud_loader.get_contents(episode=args.episode)

    rr.init(str(cloud_loader.cache_dir), recording_id="first_sample_download_time", spawn=True)
    rr.log_file_from_path(str(cloud_loader.cache_dir))


if __name__ == "__main__":
    main()
