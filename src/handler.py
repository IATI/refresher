import argparse
import library.refresher as refresher
import library.build as build

def main(args):    
    if args.type == "refresh":
        refresher.refresh()
        refresher.reload(
            args.errors
        )
    if args.type == "reload":
        refresher.reload(
            args.errors
        )
    if args.type == "build": 
        build.main()
    else:
        print("Type is required - either refresh, reload or build.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Refresh/Build from IATI Registry')
    parser.add_argument('-t', '--type', dest='type', default="refresh", help="Trigger 'refresh' or 'build'")
    parser.add_argument('-e', '--errors', dest='errors', action='store_true', default=False, help="Attempt to download previous errors")
    args = parser.parse_args()
    main(args)