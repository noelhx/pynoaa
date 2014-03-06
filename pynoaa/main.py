from datetime import date
import argparse

from data import get_interval, get_year


def main():
    init_year = 1901
    end_year = date.today().year

    # build parser
    parser = argparse.ArgumentParser(description='NOAA dataset generator tool.')
    parser.add_argument('-y', '--year', nargs='?', type=int, default=None, help='get dataset for single year.')
    parser.add_argument('-f', '--fromyear', nargs='?', type=int, default=init_year, help='initial year of the dataset.')
    parser.add_argument('-t', '--toyear', nargs='?', type=int, default=end_year, help='last year of the dataset.')

    args = parser.parse_args()

    if args.year is None:
        if args.fromyear and args.toyear:
            init_year = args.fromyear
            end_year = args.toyear
        elif args.fromyear is None and args.toyear:
            end_year = args.toyear
        elif args.fromyear and args.toyear is None:
            init_year = args.fromyear

        print("Starting retrieving data for interval: ({0}, {1})".format(init_year, end_year))
        get_interval(init_year, end_year)
    else:
        pass  #get_year(args.year)


if __name__ == "__main__":
    main()