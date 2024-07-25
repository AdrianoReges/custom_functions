import pandas as pd
from pathlib import Path
import os


def run() -> None:
    path: Path = insert_filepath()
    export_csv(path_to_file=path)


def check_pandas() -> None:
    if pd.__version__:
        pass
    else:
        os.system(command='pip install pandas==2.2.2')


def insert_filepath() -> Path:
    suffix: str = str()
    suffix_expected = '.parquet'

    while not suffix == str(object=suffix_expected):
        try:
            path_to_file: Path = Path(input('insert FULL path of filepath:'))
            suffix: str = path_to_file.suffix
            assert suffix == suffix_expected

        except:
            print(TypeError('Please insert a parquet filepath\n'))

    return path_to_file


def export_csv(path_to_file: Path) -> None:
    path_to_export: Path = Path(
        Path(
            Path(os.path.expanduser(path=path_to_file)).parent.absolute(),
            F'{path_to_file.stem}.csv',
        )
    )

    dataframe: pd.DataFrame = pd.read_parquet(path=path_to_file)

    dataframe.to_csv(
        path_or_buf=path_to_export,
        sep=';',
        encoding='utf-8',
    )
    print(
        F'''
CSV exported successfully.
path: {path_to_export}
        '''
    )


if __name__ == '__main__':
    run()
