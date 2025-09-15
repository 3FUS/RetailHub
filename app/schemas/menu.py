from pydantic import BaseModel
from typing import Optional

class MenuResponse(BaseModel):
    id: int
    name: str
    url: str
    icon: str