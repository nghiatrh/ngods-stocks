"""
vnstock connection check script.
Tests connectivity and basic functionality for key vnstock APIs.
"""

import os
import sys


def check_import():
    print("=" * 50)
    print("1. Checking vnstock import...")
    try:
        from vnstock import Listing, Quote, Company, Finance, Trading
        print("   OK - vnstock imported successfully")
        return True
    except ImportError as e:
        print(f"   FAIL - Could not import vnstock: {e}")
        print("   Run: pip install -U vnstock")
        return False


def check_listing():
    print("=" * 50)
    print("2. Checking Listing (all symbols)...")
    try:
        from vnstock import Listing
        listing = Listing(source='VCI')
        df = listing.all_symbols()
        print(f"   OK - Retrieved {len(df)} symbols")
        print(f"   Sample: {df['symbol'].head(5).tolist()}")
        return True
    except Exception as e:
        print(f"   FAIL - {e}")
        return False


def check_quote():
    print("=" * 50)
    print("3. Checking Quote (historical price for VCB)...")
    try:
        from vnstock import Quote
        quote = Quote(symbol='VCB', source='VCI')
        df = quote.history(start='2024-01-01', end='2024-01-31', interval='1D')
        print(f"   OK - Retrieved {len(df)} rows")
        print(f"   Columns: {df.columns.tolist()}")
        print(f"   Latest close: {df['close'].iloc[-1]}")
        return True
    except Exception as e:
        print(f"   FAIL - {e}")
        return False


def check_company():
    print("=" * 50)
    print("4. Checking Company (overview for VCB)...")
    try:
        from vnstock import Company
        company = Company(symbol='VCB', source='VCI')
        overview = company.overview()
        print(f"   OK - Company overview retrieved")
        print(f"   Data shape: {overview.shape}")
        return True
    except Exception as e:
        print(f"   FAIL - {e}")
        return False


def check_finance():
    print("=" * 50)
    print("5. Checking Finance (income statement for VCB)...")
    try:
        from vnstock import Finance
        finance = Finance(symbol='VCB', source='VCI')
        df = finance.income_statement(period='year')
        print(f"   OK - Income statement retrieved")
        print(f"   Data shape: {df.shape}")
        return True
    except Exception as e:
        print(f"   FAIL - {e}")
        return False


def check_trading():
    print("=" * 50)
    print("6. Checking Trading (price board)...")
    try:
        from vnstock import Trading
        df = Trading(source='VCI').price_board(['VCB', 'ACB', 'TCB'])
        print(f"   OK - Price board retrieved")
        print(f"   Data shape: {df.shape}")
        return True
    except Exception as e:
        print(f"   FAIL - {e}")
        return False


def main():
    print("\nvnstock Connection Check")
    print("=" * 50)

    api_key = os.environ.get('VNSTOCK_API_KEY')
    if api_key:
        from vnstock import register_user
        register_user(api_key=api_key)
        print(f"   Registered with API key (community mode)")
    else:
        print("   No VNSTOCK_API_KEY set, running as guest")

    results = {}

    if not check_import():
        print("\nCannot proceed without vnstock installed. Exiting.")
        sys.exit(1)

    results['listing']  = check_listing()
    results['quote']    = check_quote()
    results['company']  = check_company()
    results['finance']  = check_finance()
    results['trading']  = check_trading()

    print("=" * 50)
    print("Summary:")
    all_passed = True
    for name, passed in results.items():
        status = "OK  " if passed else "FAIL"
        print(f"   [{status}] {name}")
        if not passed:
            all_passed = False

    print("=" * 50)
    if all_passed:
        print("All checks passed. vnstock is connected and working.")
    else:
        print("Some checks failed. See details above.")
        sys.exit(1)


if __name__ == '__main__':
    main()
