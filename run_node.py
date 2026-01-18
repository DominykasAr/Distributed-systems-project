import argparse
import uvicorn
from dynamo.node_api import create_app

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--node-id", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--peers", default="", help="Comma list of peer base URLs, e.g. http://127.0.0.1:8002,http://127.0.0.1:8003")
    p.add_argument("--replication", type=int, default=2, help="R replication factor")
    p.add_argument("--w", type=int, default=1, help="Write quorum")
    p.add_argument("--q", type=int, default=1, help="Read quorum")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    peers = [x.strip() for x in args.peers.split(",") if x.strip()]
    app = create_app(
        node_id=args.node_id,
        base_url=f"http://{args.host}:{args.port}",
        peers=peers,
        replication=args.replication,
        w=args.w,
        q=args.q,
        debug=args.debug,
    )

    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()