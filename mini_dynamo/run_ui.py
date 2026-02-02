import argparse
import uvicorn
from ui.ui_app import create_ui_app

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--target", default="http://127.0.0.1:8001", help="A node base URL")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    app = create_ui_app(target_node=args.target, debug=args.debug)
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()