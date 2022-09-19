import sqlite3
import datetime as dt

accruals = [
    (1, dt.date(2022, 1, 13), 1),
    (2, dt.date(2022, 2, 12), 2),
    (3, dt.date(2022, 3, 6), 3),
    (4, dt.date(2022, 4, 9), 4),
    (5, dt.date(2022, 5, 7), 5),
    (6, dt.date(2022, 6, 8), 6),
]

payments = [
    (1, dt.date(2022, 1, 5), 1),
    (2, dt.date(2022, 1, 14), 1),
    (3, dt.date(2022, 1, 13), 1),
    (4, dt.date(2022, 7, 13), 7),
    (5, dt.date(2022, 12, 5), 12),
]


def acr_pymt(cur):
    '''
    По существу, функция выполняется в два этапа (просеивания):

    1) Каждый долг сопоставляется по месяцу с соответвующим платежом

    2) Для платежей, которым не нашлось долго из нужного месяца,
    подбираются старые долги

    В итоге результаты подбора выводятся в терминал.
    '''

    # Получение платежей и долгов сразу отсортированных в нужном порядке
    cur.execute('''
    SELECT id, month, date
    FROM payments
    ORDER BY month, date
    ;
    ''')
    payments = cur.fetchall()
    cur.execute('''
    SELECT id, month, date
    FROM accruals
    ORDER BY month, date
    ;
    ''')
    accruals = cur.fetchall()
    # Создание вспомогательных массивов
    helping_accruals = accruals.copy()  # Нужны для второго этапа
    first_lonely_pymts = payments.copy()
    paid_accruals = []  # Нужны, чтобы проверить отсеян ли платеж/долг
    used_payments = []
    table = []
    # Первое просеивание
    # Понима., что здесь сложность квадратичная, что не есть хорошо
    for acr in accruals:
        for pymt in payments:
            if (
                acr[1] == pymt[1] and
                acr[2] <= pymt[2] and
                acr not in paid_accruals and
                pymt not in used_payments
            ):
                table.append((acr[0], pymt[0]))
                paid_accruals.append(acr)
                used_payments.append(pymt)
                helping_accruals.remove(acr)
                first_lonely_pymts.remove(pymt)
                break
    # Второе просеивание
    final_lonely_payments = first_lonely_pymts.copy()
    if helping_accruals and first_lonely_pymts:
        oldest_accrual = min(helping_accruals, key=lambda i: i[1])
        for pymt in first_lonely_pymts:
            if pymt[2] >= oldest_accrual[2]:
                table.append((oldest_accrual[0], pymt[0]))
                final_lonely_payments.remove(pymt)
                helping_accruals.remove(oldest_accrual)
                oldest_accrual = None
                if helping_accruals:
                    oldest_accrual = min(helping_accruals, key=lambda i: i[1])
                else:
                    break
    # Формат вывода:
    # 1) таблица - (id долга, id платежа)
    # 2) платежи-одиночки - список кортежей с данными по каждому платежу внутри
    return table, final_lonely_payments


if __name__ == '__main__':
    con = sqlite3.connect('db.sqlite')

    cur = con.cursor()

    cur.executescript('''
    CREATE TABLE IF NOT EXISTS accruals(
        id INTEGER,
        date DATE,
        month INTEGER
    );

    CREATE TABLE IF NOT EXISTS payments(
        id INTEGER,
        date DATE,
        month INTEGER
    );
    ''')

    cur.executemany('INSERT INTO accruals VALUES(?, ?, ?);', accruals)
    cur.executemany('INSERT INTO payments VALUES(?, ?, ?);', payments)

    final_table, final_payments = (acr_pymt(cur))
    print(final_table)
    print(final_payments)
    con.commit()
    con.close()
