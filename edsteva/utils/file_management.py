from pathlib import Path

import _pickle as pickle
from loguru import logger


def save_object(obj, filename: Path):
    if not isinstance(filename, Path):
        filename = Path(filename)
    Path.mkdir(filename.parent, exist_ok=True, parents=True)
    with Path.open(filename, "wb") as outp:  # Overwrites any existing file.
        pickle.dump(obj, outp, -1)
        logger.info("Saved to {}", filename)


def load_object(filename: str):
    file = Path(filename)
    if Path.is_file(file):
        with Path.open(file, "rb") as obj:
            logger.info("Successfully loaded from {}", filename)
            return pickle.load(obj)
    else:
        raise FileNotFoundError(
            "There is no file found in {}".format(
                filename,
            )
        )


def delete_object(obj, filename: str):
    file = Path(filename)
    if Path.is_file(file):
        Path.unlink(file)
        logger.info(
            "Removed from {}",
            filename,
        )
        if hasattr(obj, "path"):
            del obj.path
    else:
        logger.warning(
            "There is no file found in {}",
            filename,
        )
