from pathlib import Path
import textwrap


class ProjectPrepper:
    """
    Utility to prepare test projects on disk.
    """

    # Path to project file
    path: Path
    # Content of project file
    yaml: str

    def __init__(self, projdir: Path, yaml: str | None = None):
        Path.mkdir(projdir, exist_ok=True)
        self.path = projdir / "makegis.project.yml"
        default_yaml = """
        defaults:
          load:
            epsg: 4326
            geom_index: false
          target: pg_dev

        targets:
          pg_dev:
            host: 127.0.0.1
            port: 5432
            user: mkgs
            db: mkgs
        """
        self.yaml = yaml if yaml is not None else default_yaml
        with open(self.path, "w") as f:
            f.write(self.yaml)

    def add_config(self, child_dir: Path, yaml: str):
        """
        Writes yaml content to a config file child dir,
        specified relative to project directory
        """
        config_path = self.path.parent / child_dir / Path("makegis.yml")
        Path.mkdir(config_path.parent, parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            f.write(textwrap.dedent(yaml))
