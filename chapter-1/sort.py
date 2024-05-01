import asyncio
import argparse
from logging import info, basicConfig, error, INFO
from aiopath import AsyncPath
from aioshutil import copyfile


async def read_folder(source_folder, output_folder):
    try:
        async for file_path in source_folder.iterdir():
            if await file_path.is_file():
                info(f"Reading file: {file_path}")
                yield file_path
            elif await file_path.is_dir():
                info(f"Reading dir: {file_path}")
                async for file_path in read_folder(file_path, output_folder):
                    yield file_path
    except Exception as e:
        error(f"Error processing folder: {e}")


async def copy_file(file_path, output_folder):
    try:
        extension = file_path.suffix[1:].lower()
        destination_folder = output_folder / extension
        await destination_folder.mkdir(parents=True, exist_ok=True)
        await copyfile(file_path, destination_folder / file_path.name)
        info(f"File copied successfully: {file_path}")
    except Exception as e:
        error(f"Error copying file {file_path}: {e}")


async def main(source_folder, output_folder):
    try:
        source_folder = AsyncPath(source_folder)
        output_folder = AsyncPath(output_folder)

        files_to_copy = [file async for file in read_folder(source_folder, output_folder)]
        await asyncio.gather(*(copy_file(file, output_folder) for file in files_to_copy))

    except Exception as e:
        error(f"Error in main function: {e}")


if __name__ == "__main__":
    basicConfig(
        level=INFO,
        filename="info.log",
        format="%(asctime)s - %(levelname)s : %(message)s")

    parser = argparse.ArgumentParser(description="Async file sorting script")
    parser.add_argument("source_folder", type=str, help="Source folder path")
    parser.add_argument("output_folder", type=str, help="Output folder path")
    args = parser.parse_args()

    asyncio.run(main(args.source_folder, args.output_folder))

    # python sort.py test/source test/sorted
