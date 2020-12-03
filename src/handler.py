import argparse
import library.refresher as refresher

def main(args):    
    if args.type == "refresh":
        refresher.refresh()
    else:       
        refresher.reload(
            args.errors
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Refresh/Reload from IATI Registry')
    parser.add_argument('-t', '--type', dest='type', default="refresh", help="Trigger 'refresh' or 'reload'")
    parser.add_argument('-e', '--errors', dest='errors', action='store_true', default=False, help="Attempt to download previous errors")
    args = parser.parse_args()
    main(args)