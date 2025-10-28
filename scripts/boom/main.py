from boom_cli import BoomCli
from boom import Boom


def main():

    boomCli = BoomCli()
    args = boomCli.parse()
    boomber = Boom(vars(args))
    boomber.run()


if __name__ == "__main__":
    main()
