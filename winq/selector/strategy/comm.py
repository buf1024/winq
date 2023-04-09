from datetime import datetime


def normalize_date(test_date) -> datetime:
    now = datetime.now()
    if test_date is None:
        test_date = datetime(year=now.year, month=now.month, day=now.day)
    else:
        ex = False
        for fmt in ['%Y-%m-%d', '%Y%m%d']:
            ex = False
            try:
                test_date = datetime.strptime(
                    test_date, fmt)
                break
            except ValueError:
                ex = True
        if ex:
            test_date = datetime(year=now.year, month=now.month, day=now.day)
    return test_date
