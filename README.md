# Rerun LeRobot Previewer

This repo demonstrates some minimal logic to pull individual episodes from a LeRobot dataset,
cache them locally, and view them with the [Rerun Viewer](https://rerun.io/docs/getting-started/what-is-rerun).

Right now only GCP is supported and configuring cloud access is left to the user.

## Example Commands

### Pixi
```console
pixi run lerobot_preview <BUCKET> <PATH_TO_DATASET> <EXACT_EPISODE_NAME>
```

### UV
```console
uv run lerobot_preview <BUCKET> <PATH_TO_DATASET> <EXACT_EPISODE_NAME>
```

### Install
```console
cd <repo_root>
pip install .
lerobot_preview <BUCKET> <PATH_TO_DATASET> <EXACT_EPISODE_NAME>
```
