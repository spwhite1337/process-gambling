from process_gambling._etl import Etl


def run(sport: str):
    api = Etl(sport=sport)



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str)
    args = parser.parse_args()
    run(sport=args.sport)

