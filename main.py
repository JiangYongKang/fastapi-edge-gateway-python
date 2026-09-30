import uvicorn

from app.api import create_app
from app.config import Settings

app = create_app(Settings.from_env())

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
