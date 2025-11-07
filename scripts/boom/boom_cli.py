import argparse


class BoomCli:

    def __init__(self):
        self.internal_parser = argparse.ArgumentParser(
            description="Boom script: a simple cli app to generate caos in your distributed system"
        )

    def parse(self) -> argparse.Namespace:
        self.internal_parser.add_argument(
            "-t",
            type=str,
            help="Your docker container name target, default uses a random image",
        )
        self.internal_parser.add_argument(
            "--mode",
            choices=["random", "target", "group"],
            default="random",
            help="The mode of operation for the boom script",
        )
        self.internal_parser.add_argument(
            "-f",
            type=str,
            help="The docker compose file to use",
        )
        return self.internal_parser.parse_args()
