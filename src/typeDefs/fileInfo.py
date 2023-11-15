from typing import TypedDict

class IFileInfo(TypedDict):
    file_tag: str
    folder_location: str
    filename: str
    date_format: str