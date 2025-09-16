"""Support LeRobot data stored in GCP."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import xxhash
from google.cloud import storage

# TODO make sure temp path is system agnostic
DEST = Path("/tmp/rerun")


def load_json_l(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def index_from_name(name: str) -> int:
    """Extracts the episode index from a name of the form 'episode_{index}'."""
    without_extension = Path(name).stem
    if not without_extension.startswith("episode_"):
        raise ValueError(f"Invalid episode name: {name}")
    try:
        return int(without_extension.split("_")[1])
    except ValueError as e:
        raise ValueError(f"Invalid episode name: {name}") from e


class GCPLeRobot:
    def __init__(self, bucket: str, prefix: Path, project: str | None) -> None:
        self._prefix = prefix
        self._project = project
        self._client = storage.Client(project=project)
        self._bucket = self._client.bucket(bucket)
        self._cache = DEST / str(xxhash.xxh64_hexdigest(f"{bucket}/{prefix}"))
        self._meta_cache = self._cache / "meta"

    @property
    def cache_dir(self) -> Path:
        """Returns the local cache directory that episodes will be stored in."""
        return self._cache

    def get_metadata(self) -> None:
        """Extracts LeRobot metadata from GCP and stores it in the local cache."""
        if not (self._meta_cache).exists():
            self._meta_cache.mkdir(parents=True, exist_ok=True)
            # Download metadata
            blobs = self._bucket.list_blobs(prefix=self._prefix / "meta")
            for blob in blobs:
                blob.download_to_filename(self._meta_cache / Path(blob.name).name)
            shutil.move(self._meta_cache / "episodes.jsonl", self._meta_cache / "rerun_all_episodes.jsonl")
            (self._meta_cache / "episodes.jsonl").touch()

            # Avoid listing chunk dirs every time
            # Need trailing slash to get subdir names
            iterator = self._bucket.list_blobs(prefix=str(Path(self._prefix) / "data") + "/", delimiter="/")
            data_subdirectories = set()
            for page in iterator.pages:
                if page.prefixes:
                    print(f"{page.prefixes=}")
                    data_subdirectories.update(page.prefixes)
            print(f"{data_subdirectories=}")
            data_subdirs = list(Path(subdir).name for subdir in data_subdirectories)

            video_subdirectories = set()
            iterator = self._bucket.list_blobs(
                prefix=str(Path(self._prefix) / "videos" / data_subdirs[0]) + "/",
                delimiter="/",
            )
            for page in iterator.pages:
                if page.prefixes:
                    video_subdirectories.update(page.prefixes)
            video_subdirs = list(Path(subdir).name for subdir in video_subdirectories)
            with open(self._meta_cache / "rerun_meta.json", "w", encoding="utf-8") as f:
                json.dump({"subdirs": list(data_subdirs), "video_subdirs": list(video_subdirs)}, f)

    def _maybe_download(self, blob: storage.Blob, dest: Path) -> None:
        if not dest.parent.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            blob.download_to_filename(dest)

    def get_contents(self, episode: str) -> None:
        """Downloads the data and video files for a given episode into the local cache."""
        potential_subdirs = json.loads((self._meta_cache / "rerun_meta.json").read_text())
        for subdir in potential_subdirs["subdirs"]:
            episode_data_path = Path(self._prefix) / "data" / subdir / f"{episode}"
            blobs = self._bucket.list_blobs(prefix=episode_data_path)
            if not blobs:
                continue
            for blob in blobs:
                dest = self._cache / "data" / subdir / Path(blob.name).name
                self._maybe_download(blob, dest)
            for video_subdir in potential_subdirs["video_subdirs"]:
                episode_video_path = Path(self._prefix) / "videos" / subdir / video_subdir / f"{episode}"
                blobs = self._bucket.list_blobs(prefix=episode_video_path)
                for blob in blobs:
                    dest = self._cache / "videos" / subdir / video_subdir / Path(blob.name).name
                    self._maybe_download(blob, dest)
            break  # Early exit because we found the episode
        all_episodes = load_json_l(self._meta_cache / "rerun_all_episodes.jsonl")
        previous_episodes = load_json_l(self._meta_cache / "episodes.jsonl")
        selected_episodes = [ep for ep in all_episodes if ep["episode_index"] == index_from_name(episode)]
        if selected_episodes[0] not in previous_episodes:
            with open(self._meta_cache / "episodes.jsonl", "a", encoding="utf-8") as outfile:
                json.dump(selected_episodes[0], outfile)
                outfile.write("\n")
