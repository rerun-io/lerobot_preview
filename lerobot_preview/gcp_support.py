"""Support LeRobot data stored in GCP."""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any

import tqdm
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
            for blob in tqdm.tqdm(blobs, desc="Downloading metadata"):
                blob.download_to_filename(self._meta_cache / Path(blob.name).name)
            shutil.move(self._meta_cache / "episodes.jsonl", self._meta_cache / "rerun_all_episodes.jsonl")
            (self._meta_cache / "episodes.jsonl").touch()

            # Avoid listing chunk dirs every time
            # Need trailing slash to get subdir names
            iterator = self._bucket.list_blobs(prefix=str(Path(self._prefix) / "data") + "/", delimiter="/")
            print("Caching data subdirectory information…")
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
            print("Caching video subdirectory information…")
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
        start = time.time()
        episode_query = Path(self._prefix) / "data" / "**" / f"{episode}.parquet"
        blobs = self._bucket.list_blobs(match_glob=episode_query)
        print("Took", time.time() - start, "seconds to list parquets")
        found_any = False
        for blob in tqdm.tqdm(blobs, desc=f"Downloading data subdirectories for episode {episode}"):
            found_any = True
            dest = self._cache / Path(blob.name).relative_to(self._prefix)
            self._maybe_download(blob, dest)
        if not found_any:
            raise ValueError(f"Episode {episode} not found at path {episode_query}")
        start = time.time()
        video_blobs = self._bucket.list_blobs(match_glob=Path(self._prefix) / "videos" / "**" / f"{episode}.mp4")
        print("Took", time.time() - start, "seconds to list videos")
        for video_blob in tqdm.tqdm(video_blobs, desc="Downloading videos"):
            dest = self._cache / Path(video_blob.name).relative_to(self._prefix)
            self._maybe_download(video_blob, dest)
        all_episodes = load_json_l(self._meta_cache / "rerun_all_episodes.jsonl")
        previous_episodes = load_json_l(self._meta_cache / "episodes.jsonl")
        selected_episodes = [ep for ep in all_episodes if ep["episode_index"] == index_from_name(episode)]
        if selected_episodes[0] not in previous_episodes:
            with open(self._meta_cache / "episodes.jsonl", "a", encoding="utf-8") as outfile:
                json.dump(selected_episodes[0], outfile)
                outfile.write("\n")
