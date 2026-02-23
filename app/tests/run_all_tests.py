from test_acknowledgements import run_test as test_acknowledgements
from test_articles import run_test as test_articles
from test_bib_varia import run_test as test_bib_varia

def print_result(name, success, table):
    if success:
        print(f"{name}: ✓")
    else:
        print(f"{name}: ✗")
        print(table)
        print()


def main():
    tests = [
        ("acknowledgements", test_acknowledgements),
        ("articles", test_articles),
        ("bib_varia", test_bib_varia),
    ]

    for name, test_func in tests:
        success, table = test_func()
        print_result(name, success, table)


if __name__ == "__main__":
    main()